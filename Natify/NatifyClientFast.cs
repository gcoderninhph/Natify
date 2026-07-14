using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using Gcoder.Collections;
using System.Threading.Tasks;
using Google.Protobuf;
using NATS.Client.Core;

namespace Natify
{
    internal class NatifyClientFast : INatifyClient
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
        private Task? _batchWorkerTask;

        private Task? _retryWorkerTask;
        private Task? _ackListenerTask;

        private readonly ConcurrentDictionary<string, byte> _processedMessages;
        private readonly ITimedCollection<string, byte> _messageTtlWheel;

        private readonly ConcurrentDictionary<string, (TaskCompletionSource<ByteString> task, CancellationTokenSource ct)>
            _replyTasks = new();

        public NatifyClientTriggers Trigger { get; } = new();

        private int _maxCount = 1000;
        private int _maxSize = 50 * 1024;
        private TimeSpan _maxWait = TimeSpan.FromMilliseconds(50);

        private readonly Channel<(string Subject, RentedBuffer Payload, string MessageType, string ReqId, string RepId)>
            _batchChannel =
                Channel.CreateUnbounded<(string, RentedBuffer, string, string, string)>();

        private CancellationTokenSource _cts;

        public static async Task<INatifyClient> Create(string url, string clientName, string groupName,
            string regionId,
            string serverNameToConnect, Config? config = null)
        {
            var na = new NatifyClientFast(url, clientName, groupName, regionId, serverNameToConnect, config);
            await na._connection.ConnectAsync();
            na.StartReliableFeatures();
            na.StartBatchWorker();
            na.OnMessageRep();
            return na;
        }

        private NatifyClientFast(string url, string clientName, string groupName, string regionId,
            string serverNameToConnect, Config? config = null)
        {
            if (config != null)
            {
                _maxCount = config.MaxCount;
                _maxSize = config.MaxSize;
                _maxWait = config.MaxWait;
            }

            _clientName = clientName;
            _groupName = groupName;
            _regionId = regionId;
            _serverNameToConnect = serverNameToConnect;
            _instanceId = Guid.NewGuid().ToString("N");

            _processedMessages = new ConcurrentDictionary<string, byte>();
            _messageTtlWheel = ITimedCollection<string, byte>.NewTimeSortSet();
            _messageTtlWheel.OnExpired += OnMessagesExpired;

            var opts = new NatsOpts
            {
                Url = url
            };
            _connection = new NatsConnection(opts);
            _cts = new CancellationTokenSource();
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
                var batches = new Dictionary<string, BatchAccumulator>();
                int currentCount = 0;
                int currentSizeBytes = 0;

                var batchStartTime = DateTime.UtcNow;

                while (currentCount < _maxCount && currentSizeBytes < _maxSize)
                {
                    var elapsed = DateTime.UtcNow - batchStartTime;
                    if (elapsed >= _maxWait)
                    {
                        break;
                    }

                    if (reader.TryRead(out var item))
                    {
                        if (!batches.TryGetValue(item.Subject, out var acc))
                        {
                            acc = new BatchAccumulator();
                            batches[item.Subject] = acc;
                        }

                        acc.Add(item.Payload, item.ReqId, item.MessageType, item.RepId);

                        currentCount++;
                        currentSizeBytes += item.Payload.Length;
                    }
                    else
                    {
                        var timeLeft = _maxWait - elapsed;

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
                        BatchAccumulator acc = kvp.Value;

                        string batchId = Guid.NewGuid().ToString("N");

                        try
                        {
                            var rented = NatifySerializer.SerializeBatchPooled(
                                acc.Payloads, acc.ReqIds, acc.MsgTypes, acc.RepIds, _instanceId);

                            Trigger.AddSent(rented.Length, acc.Count);
                            Trigger.AddBatchSent();

                            var unackedMsg = new UnackedMessage
                            {
                                Subject = subject,
                                Buffer = rented,
                                BatchId = batchId,
                                LastSent = DateTime.UtcNow,
                                RetryCount = 0
                            };

                            _unackedMessages.TryAdd(batchId, unackedMsg);

                            var headers = new NatsHeaders { ["Natify-BatchId"] = batchId };
                            await _connection.PublishAsync(subject, rented.Data, headers: headers,
                                cancellationToken: _cts.Token);
                        }
                        finally
                        {
                            acc.Dispose();
                        }
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

                    if (_unackedMessages.TryRemove(messageId, out var unacked))
                    {
                        unacked.Buffer?.Dispose();
                    }
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
                                LogError(
                                    $"[NatifyClientFast] Drop gói tin {unacked.BatchId} vì vượt quá số lần Retry.");
                                if (_unackedMessages.TryRemove(kvp.Key, out var removed))
                                {
                                    removed.Buffer?.Dispose();
                                }
                                continue;
                            }

                            unacked.LastSent = DateTime.UtcNow;
                            unacked.RetryCount++;

                            var headers = new NatsHeaders { ["Natify-BatchId"] = unacked.BatchId };
                            await _connection.PublishAsync(unacked.Subject!, unacked.Buffer!.Data, headers: headers,
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

            var exactData = NatifySerializer.SerializePooled(message);

            reqId = Guid.NewGuid().ToString("N");

            _batchChannel.Writer.TryWrite((subject, exactData, messageType, reqId, repId));
        }

        private void OnMessage(string topic, Action<Data<ByteString>>? callback, Func<Data<ByteString>, Task>? callbackAsync)
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
                            if (!_processedMessages.TryAdd(messageId, 1))
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
                            if (!string.IsNullOrEmpty(messageId))
                            {
                                _processedMessages.TryRemove(messageId, out _);
                            }
                            continue;
                        }

                        if (!string.IsNullOrEmpty(messageId))
                        {
                            Trigger.AddDedupItem();
                            _messageTtlWheel.AddOrUpdate(messageId, 1, TimeSpan.FromSeconds(10));
                            string ackSubject =
                                $"NatifyServer.{_serverNameToConnect}.{_clientName}.{_regionId}.ACK.{messageId}";
                            _ = _connection.PublishAsync(ackSubject, Array.Empty<byte>()).AsTask();
                        }

                        try
                        {
                            for (var i = 0; i < batch.Payloads.Count; i++)
                            {
                                var payloadBytes = batch.Payloads[i];
                                var instanceId = batch.FromInstanceId;
                                var reqId = batch.ReqId[i];
                                var repId = batch.RepId[i];
                                var result = new Data<ByteString>(payloadBytes, instanceId, reqId, repId);
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
                try
                {
                    if (_replyTasks.TryRemove(data.RepId, out var task))
                    {
                        task.task.SetResult(data.Value);
                        task.ct.Dispose();
                    }
                }
                catch (Exception ex)
                {
                    NatifyLogger.Error($"[NatifyClient] Error OnMessage Reply : {ex.Message}");
                }
            }, null);
        }

        public void OnMessage<T>(string topic, Action<Data<T>> callback) where T : IMessage, new()
        {
            OnMessage(topic, data =>
            {
                try
                {
                    var result = NatifySerializer.Deserialize<T>(data.Value);
                    callback(new Data<T>(result, data.InstanceId, data.ReqId, data.RepId));
                }
                catch (Exception ex)
                {
                    NatifyLogger.Error($"[NatifyClientFast] Error OnMessage : {ex.Message}");
                }
            }, null);
        }

        public void OnMessage<T>(string topic, Func<Data<T>, Task> callback) where T : IMessage, new()
        {
            OnMessage(topic, null, async data =>
            {
                try
                {
                    var result = NatifySerializer.Deserialize<T>(data.Value);
                    await callback(new Data<T>(result, data.InstanceId, data.ReqId, data.RepId));
                }
                catch (Exception ex)
                {
                    NatifyLogger.Error($"[NatifyClientFast] Error OnMessage : {ex.Message}");
                }
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
                var taskCompletionSource = new TaskCompletionSource<ByteString>();

                cancellationTokenSource.Token.Register(() =>
                {
                    if (_replyTasks.TryRemove(reqId, out var removed))
                    {
                        removed.task.TrySetException(new TimeoutException(
                            $"[NatifyClientFast] Request {reqId} timed out after {timeout.TotalMilliseconds}ms."));
                        removed.ct.Dispose();
                    }
                });

                cancellationTokenSource.CancelAfter(timeout);
                _replyTasks[reqId] = (taskCompletionSource, cancellationTokenSource);

                var result = await taskCompletionSource.Task;
                var t = NatifySerializer.Deserialize<TRes>(result);
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

        public void Tick()
        {
        }

        private static void LogError(string message)
        {
            NatifyLogger.Error(message);
        }

        public async ValueTask DisposeAsync()
        {
            if (_isDisposed) return;
            _isDisposed = true;

            _batchChannel.Writer.Complete();
            while (_batchChannel.Reader.TryRead(out var item))
            {
                item.Payload.Dispose();
            }

            if (_batchWorkerTask != null)
            {
                try
                {
                    if (await Task.WhenAny(_batchWorkerTask, Task.Delay(TimeSpan.FromSeconds(2))) == _batchWorkerTask)
                    {
                        await _batchWorkerTask;
                    }
                }
                catch
                {
                }
            }

            var waitStartTime = DateTime.UtcNow;
            while ((!_unackedMessages.IsEmpty) && (DateTime.UtcNow - waitStartTime).TotalSeconds < 2)
            {
                await Task.Delay(50);
            }

            foreach (var kvp in _unackedMessages)
            {
                if (_unackedMessages.TryRemove(kvp.Key, out var unacked))
                {
                    unacked.Buffer?.Dispose();
                }
            }

            try
            {
                _cts.Cancel();
            }
            catch
            {
            }

            foreach (var kvp in _replyTasks)
            {
                if (_replyTasks.TryRemove(kvp.Key, out var task))
                {
                    task.task.TrySetException(new ObjectDisposedException(nameof(NatifyClientFast)));
                    task.ct.Dispose();
                }
            }

            try
            {
                var tasks = new List<Task>();

                if (_retryWorkerTask != null)
                    tasks.Add(_retryWorkerTask);

                if (_ackListenerTask != null)
                    tasks.Add(_ackListenerTask);

                var allTasks = Task.WhenAll(tasks);

                if (await Task.WhenAny(allTasks, Task.Delay(TimeSpan.FromSeconds(1))) == allTasks)
                {
                    await allTasks;
                }
            }
            catch
            {
            }

            Trigger.Dispose();

            try
            {
                await _connection.DisposeAsync();
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
