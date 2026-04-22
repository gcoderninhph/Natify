using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Google.Protobuf;
using NATS.Client.Core;

#nullable enable

namespace Natify
{
    public class NatifyClientFast : IDisposable
    {
        private readonly ConcurrentDictionary<string, UnackedMessage> _unackedMessages = new();
        private readonly TimeSpan _ackTimeout = TimeSpan.FromMilliseconds(100);
        private readonly INatsConnection _connection;
        private readonly string _clientName;
        private readonly string _groupName;
        private readonly string _regionId;
        private readonly string _serverNameToConnect;
        private readonly string _instanceId;
        private bool _isDisposed = false;
        private readonly int _maxRetries = 10;
        private Task _batchWorkerTask;

        private Task _retryWorkerTask;
        private Task _ackListenerTask;

        private readonly ConcurrentDictionary<string, byte> _processedMessages;
        private readonly TimedSortedSet<string, byte> _messageTtlWheel;

        private readonly ConcurrentDictionary<string, (TaskCompletionSource<byte[]> task, CancellationTokenSource ct)>
            _replyTasks = new();

        public NatifyClientTriggers Trigger { get; } = new();

        private const int MaxCount = 1000;
        private const int MaxSize = 50 * 1024; // 50 KB
        private readonly TimeSpan MaxWait = TimeSpan.FromMilliseconds(50);

        private readonly Channel<(string Subject, byte[] Payload, string MessageType, string ReqId, string RepId)>
            _batchChannel =
                Channel.CreateUnbounded<(string, byte[], string, string, string)>();

        private readonly CancellationTokenSource _cts;

        public NatifyClientFast(string url, string clientName, string groupName, string regionId,
            string serverNameToConnect)
        {
            _clientName = clientName;
            _groupName = groupName;
            _regionId = regionId;
            _serverNameToConnect = serverNameToConnect;
            _instanceId = Guid.NewGuid().ToString("N");

            _processedMessages = new ConcurrentDictionary<string, byte>();
            _messageTtlWheel = new TimedSortedSet<string, byte>();
            _messageTtlWheel.OnExpired += OnMessagesExpired;

            var opts = new NatsOpts
            {
                Url = url
            };
            _connection = new NatsConnection(opts);

            _connection.ConnectAsync().AsTask().GetAwaiter().GetResult();

            _cts = new CancellationTokenSource();

            StartReliableFeatures();
            StartBatchWorker();
            OnMessageRep();
        }

        private void OnMessagesExpired(IReadOnlyList<(string Key, byte Value)> expiredItems)
        {
            foreach (var item in expiredItems)
            {
                _processedMessages.TryRemove(item.Key, out _);
            }

            Trigger.RemoveDedupItems(expiredItems.Count);
        }

        private void StartBatchWorker()
        {
            _batchWorkerTask = Task.Run(BatchWorkerAsync);
        }

        private async Task BatchWorkerAsync()
        {
            var reader = _batchChannel.Reader;

            while (await reader.WaitToReadAsync(_cts.Token))
            {
                var batches = new Dictionary<string, NatifyBatch>();
                int currentCount = 0;
                int currentSizeBytes = 0;

                var batchStartTime = DateTime.UtcNow;

                while (currentCount < MaxCount && currentSizeBytes < MaxSize)
                {
                    var elapsed = DateTime.UtcNow - batchStartTime;
                    if (elapsed >= MaxWait)
                    {
                        break;
                    }

                    if (reader.TryRead(out var item))
                    {
                        if (!batches.TryGetValue(item.Subject, out var batch))
                        {
                            batch = new NatifyBatch();
                            batches[item.Subject] = batch;
                        }

                        batch.Payloads.Add(ByteString.CopyFrom(item.Payload));
                        batch.ReqId.Add(item.ReqId);
                        batch.MsgType.Add(item.MessageType);
                        batch.RepId.Add(item.RepId);
                        batch.FormInstanceId = _instanceId;

                        currentCount++;
                        currentSizeBytes += item.Payload.Length;
                    }
                    else
                    {
                        var timeLeft = MaxWait - elapsed;

                        try
                        {
                            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
                            timeoutCts.CancelAfter(timeLeft);

                            await reader.WaitToReadAsync(timeoutCts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                    }
                }

                if (currentCount > 0)
                {
                    foreach (var kvp in batches)
                    {
                        string subject = kvp.Key;
                        NatifyBatch batchMsg = kvp.Value;

                        string batchId = Guid.NewGuid().ToString("N");

                        var (buffer, length) = NatifySerializer.Serialize(batchMsg);
                        var exactData = new byte[length];
                        Array.Copy(buffer, exactData, length);
                        System.Buffers.ArrayPool<byte>.Shared.Return(buffer);

                        Trigger.AddSent(length, batchMsg.Payloads.Count);
                        Trigger.AddBatchSent();

                        var unackedMsg = new UnackedMessage
                        {
                            Subject = subject,
                            Payload = exactData,
                            BatchId = batchId,
                            LastSent = DateTime.UtcNow,
                            RetryCount = 0
                        };

                        _unackedMessages.TryAdd(batchId, unackedMsg);

                        var headers = new NatsHeaders { ["Natify-BatchId"] = batchId };
                        await _connection.PublishAsync(subject, exactData, headers: headers,
                            cancellationToken: _cts.Token);
                    }
                }
            }
        }

        private void StartReliableFeatures()
        {
            string ackSubject = $"NatifyClient.{_clientName}.{_serverNameToConnect}.{_regionId}.ACK.*";
            _ackListenerTask = Task.Run(async () =>
            {
                await foreach (var msg in _connection.SubscribeAsync<byte[]>(ackSubject, cancellationToken: _cts.Token))
                {
                    var parts = msg.Subject.Split('.');
                    string messageId = parts[^1];

                    _unackedMessages.TryRemove(messageId, out _);
                }
            });

            _retryWorkerTask = Task.Run(async () =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    var now = DateTime.UtcNow;
                    foreach (var kvp in _unackedMessages)
                    {
                        var unacked = kvp.Value;
                        if (now - unacked.LastSent > _ackTimeout)
                        {
                            if (unacked.RetryCount >= _maxRetries)
                            {
                                LogError($"[NatifyClientFast] Drop gói tin {unacked.BatchId} vì vượt quá số lần Retry.");
                                _unackedMessages.TryRemove(kvp.Key, out _);
                                continue;
                            }

                            unacked.LastSent = DateTime.UtcNow;
                            unacked.RetryCount++;

                            var headers = new NatsHeaders { ["Natify-BatchId"] = unacked.BatchId };
                            await _connection.PublishAsync(unacked.Subject, unacked.Payload, headers: headers,
                                cancellationToken: _cts.Token);
                        }
                    }

                    await Task.Delay(100, _cts.Token);
                }
            });
        }

        public void Publish<T>(string topic, T message) where T : IMessage =>
            Publish(topic, message, "PUB", out _, string.Empty);

        private void Publish<T>(string topic, T message, string messageType, out string reqId, string repId)
            where T : IMessage
        {
            reqId = string.Empty;
            if (_isDisposed) return;

            string subject = NatifyTopics.GetClientPublishSubject(_serverNameToConnect, _clientName, _regionId, topic);

            var (buffer, length) = NatifySerializer.Serialize(message);
            var exactData = new byte[length];
            Array.Copy(buffer, exactData, length);
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);

            reqId = Guid.NewGuid().ToString("N");

            _batchChannel.Writer.TryWrite((subject, exactData, messageType, reqId, repId));
        }

        private void OnMessage(string topic, Action<Data<byte[]>>? callback, Func<Data<byte[]>, Task>? callbackAsync)
        {
            var subject = NatifyTopics.GetClientListenSubject(_clientName, _serverNameToConnect, _regionId, topic);

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in _connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName,
                                       cancellationToken: _cts.Token))
                    {
                        string messageId = string.Empty;
                        if (msg.Headers != null && msg.Headers.TryGetValue("Natify-BatchId", out var msgIdVal))
                        {
                            messageId = msgIdVal.ToString();
                        }

                        var payload = msg.Data ?? Array.Empty<byte>();

                        if (!string.IsNullOrEmpty(messageId))
                        {
                            string ackSubject =
                                $"NatifyServer.{_serverNameToConnect}.{_clientName}.{_regionId}.ACK.{messageId}";
                            _ = _connection.PublishAsync(ackSubject, Array.Empty<byte>()).AsTask();

                            if (_processedMessages.TryAdd(messageId, 1))
                            {
                                Trigger.AddDedupItem();
                                long expireTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10_000;
                                _messageTtlWheel.AddOrUpdate(messageId, 1, expireTimeMs);
                            }
                            else
                            {
                                continue;
                            }
                        }

                        NatifyBatch batch;
                        try
                        {
                            batch = NatifySerializer.Deserialize<NatifyBatch>(payload, payload.Length);
                            Trigger.AddReceived(payload.Length, batch.Payloads.Count);
                        }
                        catch (Exception ex)
                        {
                            Trigger.AddError();
                            LogError($"[NatifyClientFast] Error Parsing Batch: {ex.Message}");
                            continue;
                        }

                        try
                        {
                            for (var i = 0; i < batch.Payloads.Count; i++)
                            {
                                var itemBytes = batch.Payloads[i].ToByteArray();
                                var instanceId = batch.FormInstanceId;
                                var reqId = batch.ReqId[i];
                                var repId = batch.RepId[i];
                                var result = new Data<byte[]>(itemBytes, instanceId, reqId, repId);
                                callback?.Invoke(result);
                                if (callbackAsync != null) _ = callbackAsync(result);
                            }
                        }
                        catch (Exception ex)
                        {
                            Trigger.AddError();
                            LogError($"[NatifyClientFast] OnMessage Error on {topic}: {ex.Message}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
            });
        }

        private void OnMessageRep()
        {
            OnMessage($"Rep-{_instanceId}", data =>
            {
                if (_replyTasks.TryRemove(data.RepId, out var task))
                {
                    task.task.SetResult(data.Value);
                    task.ct.Dispose();
                }
            }, null);
        }
        
        public void OnMessage<T>(string topic, Action<Data<T>> callback) where T : IMessage, new()
        {
            OnMessage(topic, data =>
            {
                var result = NatifySerializer.Deserialize<T>(data.Value, data.Value.Length);
                callback(new Data<T>(result, data.InstanceId, data.ReqId, data.RepId));
            }, null);
        }

        public void OnMessage<T>(string topic, Func<Data<T>, Task> callback) where T : IMessage, new()
        {
            OnMessage(topic, null, async data =>
            {
                var result = NatifySerializer.Deserialize<T>(data.Value, data.Value.Length);
                await callback(new Data<T>(result, data.InstanceId, data.ReqId, data.RepId));
            });
        }

        public async Task<TRes> RequestAsync<TReq, TRes>(string topic, TReq requestData, TimeSpan timeout)
            where TReq : IMessage
            where TRes : IMessage, new()
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(NatifyClientFast));

            Publish(topic, requestData, "REQ", out var reqId, string.Empty);
            if (!string.IsNullOrEmpty(reqId))
            {
                var cancellationTokenSource = new CancellationTokenSource();
                var taskCompletionSource = new TaskCompletionSource<byte[]>();

                cancellationTokenSource.Token.Register(() =>
                {
                    if (_replyTasks.TryRemove(reqId, out _))
                    {
                        taskCompletionSource.TrySetException(new TimeoutException(
                            $"[NatifyClientFast] Request {reqId} timed out after {timeout.TotalMilliseconds}ms."));
                    }
                });

                cancellationTokenSource.CancelAfter(timeout);
                _replyTasks[reqId] = (taskCompletionSource, cancellationTokenSource);

                var result = await taskCompletionSource.Task;
                var t = NatifySerializer.Deserialize<TRes>(result, result.Length);
                return t;
            }

            throw new Exception($"[NatifyClientFast] Request Failed: {reqId}");
        }

        public void OnRequest<TReq, TRep>(string topic, Func<TReq, TRep> handler)
            where TReq : IMessage, new()
            where TRep : IMessage
        {
            OnMessage<TReq>(topic, tReq =>
            {
                var result = handler(tReq.Value);
                Publish($"Rep-{tReq.InstanceId}", result, "REP", out var reqId, tReq.ReqId);
            });
        }

        public void OnRequest<TReq, TRep>(string topic, Func<TReq, Task<TRep>> handlerAsync)
            where TReq : IMessage, new()
            where TRep : IMessage
        {
            OnMessage<TReq>(topic, async tReq =>
            {
                var result = await handlerAsync(tReq.Value);
                Publish($"Rep-{tReq.InstanceId}", result, "REP", out var reqId, tReq.ReqId);
            });
        }

        private void LogError(string message)
        {
            Console.WriteLine(message);
        }

        public void Dispose()
        {
            if (_isDisposed) return;
            _isDisposed = true;

            _batchChannel.Writer.Complete();

            if (_batchWorkerTask != null)
            {
                try
                {
                    _batchWorkerTask.Wait(TimeSpan.FromSeconds(2));
                }
                catch
                {
                }
            }

            var waitStartTime = DateTime.UtcNow;
            while ((!_unackedMessages.IsEmpty) && (DateTime.UtcNow - waitStartTime).TotalSeconds < 2)
            {
                Thread.Sleep(50);
            }

            try
            {
                _cts.Cancel();
            }
            catch
            {
            }

            try
            {
                var tasksToWait = new List<Task>();
                if (_retryWorkerTask != null) tasksToWait.Add(_retryWorkerTask);
                if (_ackListenerTask != null) tasksToWait.Add(_ackListenerTask);

                if (tasksToWait.Count > 0)
                    Task.WaitAll(tasksToWait.ToArray(), TimeSpan.FromSeconds(1));
            }
            catch
            {
            }

            Trigger.Dispose();

            try
            {
                _connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch
            {
            }

            _messageTtlWheel.OnExpired -= OnMessagesExpired;
            _messageTtlWheel.Dispose();

            _cts.Dispose();
        }
    }
}
