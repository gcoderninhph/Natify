using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Google.Protobuf;
using NATS.Client.Core;

#nullable enable

#if UNITY_5_3_OR_NEWER
using UnityEngine;
#endif

namespace Natify
{
    public class NatifyClient : IDisposable
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
        private readonly ConcurrentQueue<Action> _mainThreadActions = new();

        public NatifyClient(string url, string clientName, string groupName, string regionId,
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

            // Dùng GetAwaiter().GetResult() an toàn hơn Wait()
            _connection.ConnectAsync().AsTask().GetAwaiter().GetResult();

            _cts = new CancellationTokenSource();

            // Khởi chạy luồng ngầm an toàn (thay thế cho Thread truyền thống gây cảnh báo)
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

            // Vòng lặp chờ tin nhắn ĐẦU TIÊN của một lô mới
            while (await reader.WaitToReadAsync(_cts.Token))
            {
                var batches = new Dictionary<string, NatifyBatch>();
                int currentCount = 0;
                int currentSizeBytes = 0;

                // Bắt đầu bấm giờ ngay khi nhận được tin nhắn đầu tiên
                var batchStartTime = DateTime.UtcNow;

                // Vòng lặp gom hàng
                while (currentCount < MaxCount && currentSizeBytes < MaxSize)
                {
                    // Kiểm tra xem đã hết 50ms chưa
                    var elapsed = DateTime.UtcNow - batchStartTime;
                    if (elapsed >= MaxWait)
                    {
                        break; // Hết 50ms -> Cắt lô gửi luôn
                    }

                    // Cố gắng rút tin nhắn ra khỏi Phễu
                    if (reader.TryRead(out var item))
                    {
                        if (!batches.TryGetValue(item.Subject, out var batch))
                        {
                            batch = new NatifyBatch();
                            batches[item.Subject] = batch;
                        }

                        // Gom vào lô
                        batch.Payloads.Add(ByteString.CopyFrom(item.Payload));
                        batch.ReqId.Add(item.ReqId);
                        batch.MsgType.Add(item.MessageType);
                        batch.RepId.Add(item.RepId);
                        batch.FormInstanceId = _instanceId;

                        // Cập nhật bộ đếm
                        currentCount++;
                        currentSizeBytes += item.Payload.Length;
                    }
                    else
                    {
                        // Phễu tạm thời hết hàng, ta sẽ chờ thêm tin nhắn mới.
                        // NHƯNG chỉ chờ tối đa trong khoảng thời gian còn lại của 50ms.
                        var timeLeft = MaxWait - elapsed;

                        try
                        {
                            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
                            timeoutCts.CancelAfter(timeLeft);

                            // Treo luồng chờ tin nhắn mới bay vào, hoặc bị ép thức dậy khi hết timeLeft
                            await reader.WaitToReadAsync(timeoutCts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            // Lỗi này xảy ra khi:
                            // 1. Hết giờ TimeLeft (đã đủ 50ms).
                            // 2. Hoặc người dùng tắt App (_cts bị Cancel).
                            // Dù là lý do gì, ta cũng break để xả nốt hàng đang cầm trên tay xuống NATS.
                            break;
                        }
                    }
                }

                // --- XẢ BATCH (Gửi đi) ---
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

        // Khởi động luồng Retry và lắng nghe ACK trong Constructor
        private void StartReliableFeatures()
        {
            // 1. Lắng nghe ACK từ Server
            string ackSubject = $"NatifyClient.{_clientName}.{_serverNameToConnect}.{_regionId}.ACK.*";
            _ackListenerTask = Task.Run(async () =>
            {
                await foreach (var msg in _connection.SubscribeAsync<byte[]>(ackSubject, cancellationToken: _cts.Token))
                {
                    // Lấy MessageId từ Header hoặc Subject (ở đây lấy từ đuôi Subject cho nhanh)
                    var parts = msg.Subject.Split('.');
                    string messageId = parts[^1];

                    // Nhận được ACK -> Xóa khỏi danh sách chờ gửi lại
                    _unackedMessages.TryRemove(messageId, out _);
                }
            });

            // 2. Vòng lặp ngầm kiểm tra và Gửi lại (Retry)
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
                                LogError($"[NatifyClient] Drop gói tin {unacked.BatchId} vì vượt quá số lần Retry.");
                                _unackedMessages.TryRemove(kvp.Key, out _);
                                continue;
                            }

                            // Gửi lại
                            unacked.LastSent = DateTime.UtcNow;
                            unacked.RetryCount++;

                            var headers = new NatsHeaders { ["Natify-BatchId"] = unacked.BatchId };
                            await _connection.PublishAsync(unacked.Subject, unacked.Payload, headers: headers,
                                cancellationToken: _cts.Token);
                        }
                    }

                    await Task.Delay(100, _cts.Token); // Quét mỗi 100ms
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

            // Chỉ Serialize ra byte[] và ném vào Phễu, hàm này return ngay lập tức (< 0.001ms)
            var (buffer, length) = NatifySerializer.Serialize(message);
            var exactData = new byte[length];
            Array.Copy(buffer, exactData, length);
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);

            reqId = Guid.NewGuid().ToString("N");

            _batchChannel.Writer.TryWrite((subject, exactData, messageType, reqId, repId));
        }

        // Bỏ async void, thay bằng void và bọc Task bên trong
// 1. Thêm tham số requiresMainThread = true
        private void OnMessage(string topic, Action<Data<byte[]>>? callback, Func<Data<byte[]>, Task>? callbackAsync,
            bool requiresMainThread = true)
        {
            var subject = NatifyTopics.GetClientListenSubject(_clientName, _serverNameToConnect, _regionId, topic);

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in _connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName,
                                       cancellationToken: _cts.Token))
                    {
                        // SỬA LỖI 3: Trích xuất Header an toàn, chống lọt Deduplication
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
                                continue; // Đã xử lý rồi thì bỏ qua luôn
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
                            LogError($"[NatifyClient] Error Parsing Batch: {ex.Message}");
                            continue; // SỬA LỖI 2: Dùng 'continue' thay vì 'return' để luồng không bị sập!
                        }

                        // Đóng gói hành động giải nén thành Action
                        Action processBatch = () =>
                        {
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
                                LogError($"[NatifyClient] OnMessage Error on {topic}: {ex.Message}");
                            }
                        };

                        // SỬA LỖI 1: Nếu là hệ thống (Rep), xả luôn. Nếu là Game logic, cho vào hàng đợi.
                        if (requiresMainThread)
                        {
                            _mainThreadActions.Enqueue(processBatch);
                        }
                        else
                        {
                            processBatch(); // Chạy ngay lập tức không cần Tick()
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
            });
        }

// 2. Chỉnh lại hàm nghe Reply để tắt yêu cầu Main Thread
        private void OnMessageRep()
        {
            OnMessage($"Rep-{_instanceId}", data =>
            {
                if (_replyTasks.TryRemove(data.RepId, out var task))
                {
                    task.task.SetResult(data.Value);
                    task.ct.Dispose();
                }
            }, null, false); // <--- QUAN TRỌNG: requiresMainThread = false
        }
        
        public void OnMessage<T>(string topic, Action<Data<T>> callback) where T : IMessage, new()
        {
            OnMessage(topic, data =>
            {
                var result = NatifySerializer.Deserialize<T>(data.Value, data.Value.Length);
                callback(new Data<T>(result, data.InstanceId, data.ReqId, data.RepId));
            }, null, true);
        }

        public void OnMessage<T>(string topic, Func<Data<T>, Task> callback) where T : IMessage, new()
        {
            OnMessage(topic, null, async data =>
            {
                var result = NatifySerializer.Deserialize<T>(data.Value, data.Value.Length);
                await callback(new Data<T>(result, data.InstanceId, data.ReqId, data.RepId));
            }, true);
        }

        public async Task<TRes> RequestAsync<TReq, TRes>(string topic, TReq requestData, TimeSpan timeout)
            where TReq : IMessage
            where TRes : IMessage, new()
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(NatifyClient));

            Publish(topic, requestData, "REQ", out var reqId, string.Empty);
            if (!string.IsNullOrEmpty(reqId))
            {
                var cancellationTokenSource = new CancellationTokenSource();
                var taskCompletionSource = new TaskCompletionSource<byte[]>();

                // --- ĐOẠN CODE CẦN THÊM CHO SERVER ---
                cancellationTokenSource.Token.Register(() =>
                {
                    if (_replyTasks.TryRemove(reqId, out _))
                    {
                        // Ép Task ném ra TimeoutException khi hết giờ
                        taskCompletionSource.TrySetException(new TimeoutException(
                            $"[NatifyServer] Request {reqId} timed out after {timeout.TotalMilliseconds}ms."));
                    }
                });
                // --- KẾT THÚC ---

                cancellationTokenSource.CancelAfter(timeout);
                _replyTasks[reqId] = (taskCompletionSource, cancellationTokenSource);

                var result = await taskCompletionSource.Task;
                var t = NatifySerializer.Deserialize<TRes>(result, result.Length);
                return t;
            }

            throw new Exception($"[NatifyClient] Request Failed: {reqId}");
        }

        // <summary>
        /// Xử lý Request từ Server gửi xuống (Đồng bộ với Unity Main Thread qua hàm Tick)
        /// </summary>
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

        /// <summary>
        /// Xử lý Request bất đồng bộ từ Server gửi xuống (Ví dụ: Chờ Load Asset, Load File save)
        /// </summary>
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
            int count = 0;
            while (count < 100 && _mainThreadActions.TryDequeue(out var action))
            {
                action.Invoke();
                count++;
            }
        }

        private void LogError(string message)
        {
#if UNITY_5_3_OR_NEWER
            UnityEngine.Debug.LogError(message);
#else
            Console.WriteLine(message);
#endif
        }

        public void Dispose()
        {
            if (_isDisposed) return;
            _isDisposed = true;

            // BƯỚC 1: Khóa van đầu vào - Từ chối mọi lệnh Publish mới
            _batchChannel.Writer.Complete();

            // BƯỚC 2: Chờ luồng ngầm gom nốt Batch và đẩy xuống TCP
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

            // BƯỚC 3 [QUAN TRỌNG]: Chờ nhận ACK cho các gói tin cuối cùng (Y hệt Server)
            // Đảm bảo những tin nhắn Client vừa xả ở Bước 2 được Server xác nhận
            var waitStartTime = DateTime.UtcNow;
            while ((!_unackedMessages.IsEmpty) && (DateTime.UtcNow - waitStartTime).TotalSeconds < 2)
            {
                Thread.Sleep(50);
            }

            // BƯỚC 4: Sập cầu dao (Hủy Token) để ngắt các vòng lặp while/foreach
            try
            {
                _cts.Cancel();
            }
            catch
            {
            }

            // BƯỚC 5: Đợi các luồng nền Retry và ACK Listener tắt hẳn
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

            // BƯỚC 6: Giải phóng tài nguyên, cắt mạng
            Trigger.Dispose();

            try
            {
                _connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch
            {
            }

            // BƯỚC 7 [VÁ MEMORY LEAK]: Dọn dẹp Time Wheel
            _messageTtlWheel.OnExpired -= OnMessagesExpired;
            _messageTtlWheel.Dispose();

            _cts.Dispose();
        }
    }
}