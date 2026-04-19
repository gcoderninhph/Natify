using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Google.Protobuf;
using NATS.Client.Core;

#if UNITY_5_3_OR_NEWER
using UnityEngine;
#endif

namespace Natify
{
    public class UnackedMessage
    {
        public string Subject { get; set; }
        public byte[] Payload { get; set; }
        public string MessageId { get; set; }
        public DateTime LastSent { get; set; }
        public int RetryCount { get; set; }
    }

    public class NatifyClient : IDisposable
    {
        private readonly ConcurrentDictionary<string, UnackedMessage> _unackedMessages = new();
        private readonly TimeSpan _ackTimeout = TimeSpan.FromMilliseconds(100);
        private readonly INatsConnection _connection;
        private readonly string _clientName;
        private readonly string _groupName;
        private readonly string _regionId;
        private readonly string _serverNameToConnect;
        private bool _isDisposed = false;
        private readonly int _maxRetries = 10;
        private Task _batchWorkerTask;

        private Task _retryWorkerTask;
        private Task _ackListenerTask;

        private readonly ConcurrentDictionary<string, byte> _processedMessages;
        private readonly TimedSortedSet<string, byte> _messageTtlWheel;

        public NatifyClientTriggers Trigger { get; } = new();

        private const int MaxCount = 1000;
        private const int MaxSize = 50 * 1024; // 50 KB
        private readonly TimeSpan MaxWait = TimeSpan.FromMilliseconds(50);

        private readonly Channel<(string Subject, byte[] Payload)> _batchChannel =
            Channel.CreateUnbounded<(string, byte[])>();

        private readonly CancellationTokenSource _cts;
        private readonly ConcurrentQueue<Action> _mainThreadActions = new();

        public NatifyClient(string url, string clientName, string groupName, string regionId,
            string serverNameToConnect)
        {
            _clientName = clientName;
            _groupName = groupName;
            _regionId = regionId;
            _serverNameToConnect = serverNameToConnect;

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
                        string messageId = Guid.NewGuid().ToString("N");

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
                            MessageId = messageId,
                            LastSent = DateTime.UtcNow,
                            RetryCount = 0
                        };

                        _unackedMessages.TryAdd(messageId, unackedMsg);

                        var headers = new NatsHeaders { ["Natify-MsgId"] = messageId };
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
                                LogError($"[NatifyClient] Drop gói tin {unacked.MessageId} vì vượt quá số lần Retry.");
                                _unackedMessages.TryRemove(kvp.Key, out _);
                                continue;
                            }

                            // Gửi lại
                            unacked.LastSent = DateTime.UtcNow;
                            unacked.RetryCount++;

                            var headers = new NatsHeaders { ["Natify-MsgId"] = unacked.MessageId };
                            await _connection.PublishAsync(unacked.Subject, unacked.Payload, headers: headers,
                                cancellationToken: _cts.Token);
                        }
                    }

                    await Task.Delay(100, _cts.Token); // Quét mỗi 100ms
                }
            });
        }

        public void Publish<T>(string topic, T message) where T : IMessage
        {
            if (_isDisposed) return;

            string subject = NatifyTopics.GetClientPublishSubject(_serverNameToConnect, _clientName, _regionId, topic);

            // Chỉ Serialize ra byte[] và ném vào Phễu, hàm này return ngay lập tức (< 0.001ms)
            var (buffer, length) = NatifySerializer.Serialize(message);
            var exactData = new byte[length];
            Array.Copy(buffer, exactData, length);
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);

            _batchChannel.Writer.TryWrite((subject, exactData));
        }

        // Bỏ async void, thay bằng void và bọc Task bên trong
        public void OnMessage<T>(string topic, Action<T> callback) where T : IMessage, new()
        {
            var subject = NatifyTopics.GetClientListenSubject(_clientName, _serverNameToConnect, _regionId, topic);

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in _connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName,
                                       cancellationToken: _cts.Token))
                    {
                        // A. XỬ LÝ ĐỘ TIN CẬY (ACK & DEDUPLICATION)
                        msg.Headers?.TryGetValue("Natify-MsgId", out var messageId);
                        var payload = msg.Data ?? Array.Empty<byte>();

                        // BÁO CÁO: Ghi nhận số lượng bytes nhận được
                        Trigger.AddReceived(payload.Length, 1);

                        if (!string.IsNullOrEmpty(messageId))
                        {
                            // 1. Bắn ACK báo cho Server biết đã nhận
                            string ackSubject =
                                $"NatifyServer.{_serverNameToConnect}.{_clientName}.{_regionId}.ACK.{messageId}";
                            _ = _connection.PublishAsync(ackSubject, Array.Empty<byte>()).AsTask();

                            // 2. Kiểm tra trùng lặp
                            if (_processedMessages.TryAdd(messageId, 1))
                            {
                                // BÁO CÁO: Thêm vào bộ đếm Cache
                                Trigger.AddDedupItem();

                                // 3. Cho vào TimeWheel đếm ngược 10 giây tự xóa
                                long expireTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10_000;
                                _messageTtlWheel.AddOrUpdate(messageId, 1, expireTimeMs);
                            }
                            else
                            {
                                // Trùng lặp -> Bỏ qua
                                continue;
                            }
                        }

                        // B. XỬ LÝ LOGIC GAME (Đưa lên Main Thread của Client/Unity)
                        _mainThreadActions.Enqueue(() =>
                        {
                            try
                            {
                                var payload = msg.Data ?? Array.Empty<byte>();
                                var data = NatifySerializer.Deserialize<T>(payload, payload.Length);
                                callback(data);
                            }
                            catch (Exception ex)
                            {
                                Trigger.AddError();
                                LogError($"[NatifyClient] OnMessageReliable Error on {topic}: {ex.Message}");
                            }
                        });
                    }
                }
                catch (OperationCanceledException)
                {
                }
            });
        }

        public async Task<TRes> RequestAsync<TReq, TRes>(string topic, TReq requestData, TimeSpan timeout)
            where TReq : IMessage
            where TRes : IMessage, new()
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(NatifyClient));

            var subject = NatifyTopics.GetClientPublishSubject(_serverNameToConnect, _clientName, _regionId, topic);
            var (reqBuffer, reqLength) = NatifySerializer.Serialize(requestData);

            var exactPayload = new byte[reqLength];
            Array.Copy(reqBuffer, exactPayload, reqLength);
            System.Buffers.ArrayPool<byte>.Shared.Return(reqBuffer);

            try
            {
                var reply = await _connection.RequestAsync<byte[], byte[]>(
                    subject, exactPayload,
                    replyOpts: new NatsSubOpts { Timeout = timeout },
                    cancellationToken: _cts.Token);

                var replyPayload = reply.Data ?? Array.Empty<byte>();
                return NatifySerializer.Deserialize<TRes>(replyPayload, replyPayload.Length);
            }
            catch (NatsNoReplyException)
            {
                throw new TimeoutException(
                    $"Request trên topic '{topic}' đã bị Timeout sau {timeout.TotalMilliseconds}ms.");
            }
            catch (NatsNoRespondersException) // THÊM DÒNG NÀY VÀO ĐÂY
            {
                throw new TimeoutException(
                    $"Nhanh: Không có Server nào đang lắng nghe topic '{topic}'. Request bị Timeout lập tức.");
            }
        }

        // <summary>
        /// Xử lý Request từ Server gửi xuống (Đồng bộ với Unity Main Thread qua hàm Tick)
        /// </summary>
        public void OnRequest<TReq, TRep>(string topic, Func<TReq, TRep> handler)
            where TReq : IMessage, new()
            where TRep : IMessage
        {
            var subject = NatifyTopics.GetClientListenSubject(_clientName, _serverNameToConnect, _regionId, topic);

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in _connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName,
                                       cancellationToken: _cts.Token))
                    {
                        // Bỏ qua nếu không có hòm thư trả lời (Không phải Request)
                        if (string.IsNullOrEmpty(msg.ReplyTo)) continue;

                        // Lưu lại ReplyTo ngay lập tức vì msg có thể bị NATS thu hồi
                        string replyTo = msg.ReplyTo;
                        var payload = msg.Data ?? Array.Empty<byte>();

                        // Đẩy vào Queue để Unity Tick() xử lý an toàn trên Main Thread
                        _mainThreadActions.Enqueue(() =>
                        {
                            try
                            {
                                // 1. Giải mã Request
                                var requestData = NatifySerializer.Deserialize<TReq>(payload, payload.Length);

                                // 2. Chạy logic Game của bạn (An toàn gọi Unity API ở đây)
                                TRep replyData = handler(requestData);

                                // 3. Mã hóa kết quả
                                var (replyBuffer, replyLength) = NatifySerializer.Serialize(replyData);
                                var exactReplyData = new byte[replyLength];
                                Array.Copy(replyBuffer, exactReplyData, replyLength);
                                System.Buffers.ArrayPool<byte>.Shared.Return(replyBuffer);

                                // 4. Bắn trả kết quả cho Server (Fire and forget để không kẹt Main Thread)
                                _ = _connection.PublishAsync(replyTo, exactReplyData).AsTask();
                            }
                            catch (Exception ex)
                            {
                                LogError($"[NatifyClient] OnRequest Error on {topic}: {ex.Message}");
                            }
                        });
                    }
                }
                catch (OperationCanceledException)
                {
                    /* Client đóng */
                }
                catch (Exception ex)
                {
                    LogError($"[NatifyClient] Subscribe Request Error: {ex.Message}");
                }
            });
        }

        /// <summary>
        /// Xử lý Request bất đồng bộ từ Server gửi xuống (Ví dụ: Chờ Load Asset, Load File save)
        /// </summary>
        public void OnRequest<TReq, TRep>(string topic, Func<TReq, Task<TRep>> handlerAsync)
            where TReq : IMessage, new()
            where TRep : IMessage
        {
            var subject = NatifyTopics.GetClientListenSubject(_clientName, _serverNameToConnect, _regionId, topic);

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in _connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName,
                                       cancellationToken: _cts.Token))
                    {
                        if (string.IsNullOrEmpty(msg.ReplyTo)) continue;

                        string replyTo = msg.ReplyTo;
                        var payload = msg.Data ?? Array.Empty<byte>();

                        _mainThreadActions.Enqueue(() =>
                        {
                            // Bắt đầu thực thi Task bất đồng bộ trên Main Thread một cách an toàn
                            // Không dùng 'async void' ở đây để tránh crash app nếu user code có lỗi
                            _ = ProcessClientAsyncRequest(topic, replyTo, payload, handlerAsync);
                        });
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    LogError($"[NatifyClient] Subscribe Async Request Error on {topic}: {ex.Message}");
                }
            });
        }

        // Hàm Helper để xử lý vòng đời của một Async Request
        private async Task ProcessClientAsyncRequest<TReq, TRep>(string topic, string replyTo, byte[] payload,
            Func<TReq, Task<TRep>> handlerAsync)
            where TReq : IMessage, new()
            where TRep : IMessage
        {
            try
            {
                // 1. Giải mã Request
                var requestData = NatifySerializer.Deserialize<TReq>(payload, payload.Length);

                // 2. Chạy logic bất đồng bộ của Game (Hàm này sẽ bắt đầu chạy trên Main Thread)
                TRep replyData = await handlerAsync(requestData);

                // 3. Mã hóa kết quả
                var (replyBuffer, replyLength) = NatifySerializer.Serialize(replyData);
                var exactReplyData = new byte[replyLength];
                Array.Copy(replyBuffer, exactReplyData, replyLength);
                System.Buffers.ArrayPool<byte>.Shared.Return(replyBuffer);

                // 4. Trả lời Server
                await _connection.PublishAsync(replyTo, exactReplyData).AsTask();
            }
            catch (Exception ex)
            {
                LogError($"[NatifyClient] Async OnRequest Error on {topic}: {ex.Message}");
            }
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

        private void LogCallbackException(string topic, Exception ex)
        {
            var st = new StackTrace(ex, true);
            var userCodeFrame = st.GetFrame(0);
            string fileName = userCodeFrame?.GetFileName() ?? "Unknown File";
            int line = userCodeFrame?.GetFileLineNumber() ?? 0;

            string errorMsg =
                $"[Natify] Error in Callback of Topic '{topic}'\nFile: {fileName} (Line: {line})\nError: {ex.Message}";
            LogError(errorMsg);
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