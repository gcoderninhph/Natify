using System.Collections.Concurrent;
using Gcoder.Collections;
using Google.Protobuf;
using NATS.Client.Core;

namespace Natify;

internal class NatifyClient : INatifyClient
{
    private readonly Dictionary<string, UnackedMessage> _unackedMessages = new();
    private readonly TimeSpan _ackTimeout = TimeSpan.FromMilliseconds(100);
    private readonly INatsConnection _connection;
    private readonly string _clientName;
    private readonly string _groupName;
    private readonly string _regionId;
    private readonly string _serverNameToConnect;
    private readonly string _instanceId;
    private bool _isDisposed;
    private readonly int _maxRetries = 10;


    private List<Task> _publishList = [];
    private List<Task> _publishListSnapshot = [];
    private Task _publishFlushTask = Task.CompletedTask;

    private readonly HashSet<string> _processedMessages;
    private readonly ITimedCollection<string, Action> _messageTtlWheel;
    private readonly TimeSpan _processedMessageExpTime = TimeSpan.FromSeconds(10);

    private readonly Dictionary<string, Action<ByteString>> _replyAction = new();

    public NatifyClientTriggers Trigger { get; } = new();

    private int _maxCount = 1000;
    private int _maxSize = 50 * 1024;
    private TimeSpan _maxWait = TimeSpan.FromMilliseconds(50);

    private readonly Queue<BatchMessage> _messagePublishQueue = new();
    private readonly Dictionary<string, BatchAccumulator> _batchToSend = new();

    private CancellationTokenSource _cts;
    private CancellationTokenSource _subscribeCts;
    private readonly ConcurrentQueue<Action> _mainThreadActions = new();

    internal static async Task<INatifyClient> Create(string url, string clientName, string groupName,
        string regionId,
        string serverNameToConnect, Config? config = null)
    {
        var nc = new NatifyClient(url, clientName, groupName, regionId, serverNameToConnect, config);
        await nc._connection.ConnectAsync();
        nc.StartReliableFeatures();
        nc.OnMessageRep();
        return nc;
    }

    private NatifyClient(string url, string clientName, string groupName, string regionId,
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

        _processedMessages = [];

        _messageTtlWheel = ITimedCollection<string, Action>.NewTimeSortSet();
        _messageTtlWheel.OnExpired += OnMessagesExpired;

        var opts = new NatsOpts
        {
            Url = url
        };
        _connection = new NatsConnection(opts);
        _cts = new CancellationTokenSource();
        _subscribeCts = new CancellationTokenSource();
    }

    private void OnMessagesExpired(IReadOnlyList<(string Key, Action Value)> expiredItems)
    {
        foreach (var item in expiredItems)
        {
            try
            {
                item.Value();
            }
            catch
            {
                Trigger.AddError();
                NatifyLogger.Error(
                    "private void OnMessagesExpired(IReadOnlyList<(string Key, Action Value)> expiredItems)");
            }
        }

        Trigger.RemoveDedupItems(expiredItems.Count);
    }

    // Lắng nghe atk từ server
    private void StartReliableFeatures()
    {
        string ackSubject = $"NatifyClient.{_clientName}.{_serverNameToConnect}.{_regionId}.ACK.*";
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var msg in _connection.SubscribeAsync<byte[]>(ackSubject,
                                   cancellationToken: _cts.Token))
                {
                    var parts = msg.Subject.Split('.');
                    string messageId = parts[^1];
                    _mainThreadActions.Enqueue(() =>
                    {
                        if (_unackedMessages.TryGetValue(messageId, out var unacked))
                        {
                            unacked.Buffer?.Dispose();
                            _unackedMessages.Remove(messageId);
                        }
                    });
                }
            }
            catch (OperationCanceledException)
            {
                // shutdown bình thường
            }
        });
    }

    private DateTime _batchWorkerLastTick = DateTime.UtcNow;

    private void BatchWorkerTick(bool force = false)
    {
        var now = DateTime.UtcNow;
        if (now - _batchWorkerLastTick < _maxWait && !force) return;
        _batchWorkerLastTick = now;

        while (_messagePublishQueue.Count > 0)
        {
            _batchToSend.Clear();
            int currentCount = 0;
            int currentSizeBytes = 0;

            var batchStartTime = DateTime.UtcNow;

            while (currentCount < _maxCount && currentSizeBytes < _maxSize &&
                   _messagePublishQueue.TryDequeue(out var item))
            {
                var elapsed = DateTime.UtcNow - batchStartTime;
                if (elapsed >= _maxWait)
                {
                    break;
                }

                if (!_batchToSend.TryGetValue(item.Subject, out var accExits))
                {
                    accExits = new BatchAccumulator();
                    _batchToSend[item.Subject] = accExits;
                }

                accExits.Add(item.Payload, item.ReqId, item.MessageType, item.RepId);

                currentCount++;
                currentSizeBytes += item.Payload.Length;
            }

            if (currentCount > 0)
            {
                foreach (var kvp in _batchToSend)
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
                        _publishList.Add(_connection.PublishAsync(subject, rented.Data, headers: headers,
                            cancellationToken: _cts.Token).AsTask());
                    }
                    finally
                    {
                        acc.Dispose();
                    }
                }
            }
        }
    }

    private DateTime _retryWorkerLastTime = DateTime.UtcNow;
    private TimeSpan _retryWorkerTime = TimeSpan.FromMilliseconds(100);

    private void RetryWorkerTick(bool force = false)
    {
        var now = DateTime.UtcNow;
        if (now - _retryWorkerLastTime < _retryWorkerTime && !force) return;
        _retryWorkerLastTime = now;

        foreach (var kvp in _unackedMessages)
        {
            var unacked = kvp.Value;
            if (now - unacked.LastSent > _ackTimeout)
            {
                if (unacked.RetryCount >= _maxRetries)
                {
                    LogError(
                        $"[NatifyClient] Drop gói tin {unacked.BatchId} vì vượt quá số lần Retry.");
                    if (_unackedMessages.TryGetValue(kvp.Key, out var removed))
                    {
                        removed.Buffer?.Dispose();
                        _unackedMessages.Remove(kvp.Key);
                    }

                    continue;
                }

                unacked.LastSent = DateTime.UtcNow;
                unacked.RetryCount++;

                var headers = new NatsHeaders { ["Natify-BatchId"] = unacked.BatchId };
                _publishList.Add(_connection.PublishAsync(unacked.Subject!, unacked.Buffer!.Data, headers: headers,
                    cancellationToken: _cts.Token).AsTask());
            }
        }
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
        var reqIdAtomic = reqId;
        _mainThreadActions.Enqueue(() =>
            _messagePublishQueue.Enqueue(new BatchMessage(subject, exactData, messageType, reqIdAtomic, repId)));
    }

    private void OnMessage(string topic, Action<Data<ByteString>>? callback,
        Func<Data<ByteString>, Task>? callbackAsync)
    {
        var subject = NatifyTopics.GetClientListenSubject(_clientName, _serverNameToConnect, _regionId, topic);

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var msg in _connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName,
                                   cancellationToken: _subscribeCts.Token))
                {
                    string messageId = string.Empty;
                    if (msg.Headers != null && msg.Headers.TryGetValue("Natify-BatchId", out var msgIdVal))
                    {
                        messageId = msgIdVal.ToString();
                    }

                    var payload = msg.Data ?? [];

                    _mainThreadActions.Enqueue(() =>
                    {
                        if (!string.IsNullOrEmpty(messageId))
                        {
                            if (!_processedMessages.Add(messageId))
                            {
                                return;
                            }
                        }

                        if (!CreateBatch(payload, messageId, out var batch)) return;
                        AtkMessage(messageId);

                        try
                        {
                            for (var i = 0; i < batch.Payloads.Count; i++)
                            {
                                var payload2 = batch.Payloads[i];
                                var instanceId = batch.FromInstanceId;
                                var reqId = batch.ReqId[i];
                                var repId = batch.RepId[i];
                                var result = new Data<ByteString>(payload2, instanceId, reqId, repId);
                                callback?.Invoke(result);
                                _ = callbackAsync?.Invoke(result);
                            }
                        }
                        catch (Exception ex)
                        {
                            Trigger.AddError();
                            LogError($"[NatifyClient] OnMessage Error on {topic}: {ex.Message}");
                        }
                    });
                }
            }
            catch (OperationCanceledException)
            {
            }
        });
    }

    private void AtkMessage(string messageId)
    {
        if (!string.IsNullOrEmpty(messageId))
        {
            Trigger.AddDedupItem();
            _messageTtlWheel.AddOrUpdate(messageId, () => _processedMessages.Remove(messageId),
                _processedMessageExpTime);
            string ackSubject = $"NatifyServer.{_serverNameToConnect}.{_clientName}.{_regionId}.ACK.{messageId}";
            _publishList.Add(_connection.PublishAsync(ackSubject, Array.Empty<byte>()).AsTask());
        }
    }

    private async Task WaitAllAckAsync()
    {
        await _publishFlushTask;

        if (_publishList.Count == 0)
            return;

        (_publishList, _publishListSnapshot) = (_publishListSnapshot, _publishList);
        try
        {
            _publishFlushTask = Task.WhenAll(_publishListSnapshot);
            await _publishFlushTask;
        }
        finally
        {
            _publishListSnapshot.Clear();
        }
    }

    private bool CreateBatch(byte[] payload, string messageId, out NatifyBatch batch)
    {
        try
        {
            batch = NatifySerializer.Deserialize<NatifyBatch>(payload, payload.Length);
            Trigger.AddReceived(payload.Length, batch.Payloads.Count);
        }
        catch (Exception ex)
        {
            Trigger.AddError();
            LogError($"[NatifyClient] Error Parsing Batch: {ex.Message}");
            if (!string.IsNullOrEmpty(messageId))
            {
                _processedMessages.Remove(messageId);
            }

            batch = null!;
            return false;
        }

        return true;
    }

    private void OnMessageRep()
    {
        OnMessage($"Rep-{_instanceId}", data =>
        {
            if (_replyAction.TryGetValue(data.RepId, out var task))
            {
                task(data.Value);
                _replyAction.Remove(data.RepId);
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
            catch
            {
                Trigger.AddError();
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
            catch
            {
                Trigger.AddError();
            }
        });
    }

    public async Task<TRes> RequestAsync<TReq, TRes>(string topic, TReq requestData, TimeSpan timeout)
        where TReq : IMessage
        where TRes : IMessage, new()
    {
        if (_isDisposed) throw new ObjectDisposedException(nameof(NatifyClient));

        Publish(topic, requestData, "REQ", out var reqId, string.Empty);
        if (!string.IsNullOrEmpty(reqId))
        {
            var taskCompletionSource = new TaskCompletionSource<ByteString>();

            _mainThreadActions.Enqueue(() =>
            {
                _messageTtlWheel.AddOrUpdate(reqId, () =>
                {
                    taskCompletionSource.SetException(new TimeoutException($"[NatifyClient] Timeout: {reqId}"));
                    _replyAction.Remove(reqId);
                }, timeout);

                _replyAction[reqId] = byteString =>
                {
                    _messageTtlWheel.Remove(reqId);
                    taskCompletionSource.TrySetResult(byteString);
                    _replyAction.Remove(reqId);
                };
            });

            var result = await taskCompletionSource.Task;
            var t = NatifySerializer.Deserialize<TRes>(result);
            return t;
        }

        throw new Exception($"[NatifyClient] Request Failed: {reqId}");
    }

    public void OnRequest<TReq, TRep>(string topic, Func<TReq, TRep> handler)
        where TReq : IMessage, new()
        where TRep : IMessage
    {
        OnMessage<TReq>(topic, tReq =>
        {
            var result = handler(tReq.Value);
            Publish($"Rep-{tReq.InstanceId}", result, "REP", out _, tReq.ReqId);
        });
    }

    public void OnRequest<TReq, TRep>(string topic, Func<TReq, Task<TRep>> handlerAsync)
        where TReq : IMessage, new()
        where TRep : IMessage
    {
        OnMessage<TReq>(topic, async tReq =>
        {
            var result = await handlerAsync(tReq.Value);
            Publish($"Rep-{tReq.InstanceId}", result, "REP", out _, tReq.ReqId);
        });
    }

    private void DrainMainThreadActions()
    {
        while (_mainThreadActions.TryDequeue(out var action))
        {
            try
            {
                action.Invoke();
            }
            catch
            {
                Trigger.AddError();
            }
        }
    }

    public void Tick()
    {
        DrainMainThreadActions();
        BatchWorkerTick();
        RetryWorkerTick();
        _messageTtlWheel.Tick();
        _ = WaitAllAckAsync();
    }

    private static void LogError(string message)
    {
        NatifyLogger.Error(message);
    }


    private async Task FlushPublishAsync()
    {
        while (true)
        {
            if (_publishList.Count == 0 &&
                _publishFlushTask.IsCompleted)
                return;

            await WaitAllAckAsync();
        }
    }

    private async Task WaitServerAckAsync(TimeSpan timeout)
    {
        var start = DateTime.UtcNow;

        while (_unackedMessages.Count > 0)
        {
            RetryWorkerTick(force: true);

            await FlushPublishAsync();

            DrainMainThreadActions();

            if (DateTime.UtcNow - start > timeout)
                break;

            await Task.Delay(20);
        }
    }

    private void Clean()
    {
        foreach (var pair in _unackedMessages)
        {
            pair.Value.Buffer?.Dispose();
        }

        _unackedMessages.Clear();

        while (_messagePublishQueue.TryDequeue(out var batch))
        {
            batch.Payload.Dispose();
        }

        var actions = _messageTtlWheel.Clear();
        if (actions.Count > 0)
        {
            foreach (var action in actions)
            {
                try
                {
                    action.Value();
                }
                catch
                {
                    Trigger.AddError();
                    NatifyLogger.Error(
                        "var actions = _messageTtlWheel.Clear();\n        if (actions.Count > 0)\n        {\n            foreach (var action in actions)\n            {\n                try\n                {\n                    action.Value();\n                }\n                catch\n                {....");
                }
            }
        }

        _replyAction.Clear();
        _messageTtlWheel.OnExpired -= OnMessagesExpired;
        _messageTtlWheel.Dispose();
        Trigger.Dispose();
        _cts.Dispose();
        _subscribeCts.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_isDisposed) return;
        _isDisposed = true;

        _subscribeCts.Cancel();

        while (true)
        {
            DrainMainThreadActions();
            BatchWorkerTick(true);

            if (_mainThreadActions.IsEmpty &&
                _messagePublishQueue.Count == 0)
                break;
        }

        await FlushPublishAsync();
        await WaitServerAckAsync(TimeSpan.FromSeconds(5));
        _cts.Cancel();
        await _connection.DisposeAsync();
        Clean();
    }
}