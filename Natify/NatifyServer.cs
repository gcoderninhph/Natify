using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Google.Protobuf;
using NATS.Client.Core;

namespace Natify
{
    public class NatifyServer : IDisposable
    {
        private readonly ConcurrentDictionary<string, byte> _processedMessages = new();
        private readonly TimedSortedSet<string, byte> _messageTtlWheel = new();

        private readonly Channel<(string Subject, byte[] Payload)> _batchChannel =
            Channel.CreateUnbounded<(string, byte[])>();

        private Task _batchWorkerTask;
        private const int MaxCount = 1000;
        private const int MaxSize = 50 * 1024; // 50 KB
        private readonly TimeSpan MaxWait = TimeSpan.FromMilliseconds(50);

        private readonly ConcurrentDictionary<string, UnackedMessage> _unackedMessages = new();
        private readonly TimeSpan _ackTimeout = TimeSpan.FromMilliseconds(100);
        private readonly int _maxRetries = 10;
        public NatifyServerTriggers Trigger { get; } = new();

        private readonly INatsConnection _connection;
        private readonly string _serverName;
        private readonly string _groupName;
        private readonly string _clientNameToConnect;
        private readonly CancellationTokenSource _cts;
        private bool _isDisposed = false;

        private Task _retryWorkerTask;
        private Task _ackListenerTask;

        public NatifyServer(string url, string serverName, string groupName, string clientNameToConnect)
        {
            _serverName = serverName;
            _groupName = groupName;
            _clientNameToConnect = clientNameToConnect;
            _messageTtlWheel.OnExpired += OnMessagesExpired;

            var opts = new NatsOpts { Url = url };
            _connection = new NatsConnection(opts);

            _connection.ConnectAsync().AsTask().GetAwaiter().GetResult();
            _cts = new CancellationTokenSource();
            StartServerReliablePublishFeatures();
            _batchWorkerTask = Task.Run(BatchWorkerAsync);
        }

        private void OnMessagesExpired(IReadOnlyList<(string Key, byte Value)> expiredItems)
        {
            // Khi Time Wheel báo có item hết hạn (đã qua 10s), ta xóa nó khỏi Dictionary
            foreach (var item in expiredItems)
            {
                _processedMessages.TryRemove(item.Key, out _);
            }

            Trigger.RemoveDedupItems(expiredItems.Count);
        }

        private void StartServerReliablePublishFeatures()
        {
            // 1. Luồng lắng nghe ACK từ Client gửi lên
            // Client sẽ gửi vào: NatifyServer.{serverName}.{clientName}.{regionId}.ACK.{messageId}
            string ackSubject = $"NatifyServer.{_serverName}.{_clientNameToConnect}.*.ACK.*";

            _ackListenerTask = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in _connection.SubscribeAsync<byte[]>(ackSubject,
                                       cancellationToken: _cts.Token))
                    {
                        var parts = msg.Subject.Split('.');
                        string messageId = parts[^1];

                        // Nhận được ACK -> Xóa khỏi hàng đợi
                        _unackedMessages.TryRemove(messageId, out _);
                    }
                }
                catch (OperationCanceledException)
                {
                }
            });

            // 2. Vòng lặp Retry ngầm của Server
            _retryWorkerTask = Task.Run(async () =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    try
                    {
                        var now = DateTime.UtcNow;
                        foreach (var kvp in _unackedMessages)
                        {
                            var unacked = kvp.Value;
                            if (now - unacked.LastSent > _ackTimeout)
                            {
                                if (unacked.RetryCount >= _maxRetries)
                                {
                                    Console.WriteLine(
                                        $"[NatifyServer] Drop gói tin {unacked.MessageId} gửi tới Client vì vượt quá Retry.");
                                    _unackedMessages.TryRemove(kvp.Key, out _);
                                    continue;
                                }

                                unacked.LastSent = DateTime.UtcNow;
                                unacked.RetryCount++;

                                var headers = new NatsHeaders { ["Natify-MsgId"] = unacked.MessageId };
                                await _connection.PublishAsync(unacked.Subject, unacked.Payload, headers: headers,
                                    cancellationToken: _cts.Token);
                            }
                        }

                        await Task.Delay(100, _cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            });
        }

        public void Publish<T>(string topic, string regionId, T message) where T : IMessage
        {
            if (_isDisposed) return;

            var subject = NatifyTopics.GetServerPublishSubject(_clientNameToConnect, _serverName, regionId, topic);
    
            var (buffer, length) = NatifySerializer.Serialize(message);
            var exactData = new byte[length];
            Array.Copy(buffer, exactData, length);
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);

            // Bắn vào Phễu
            _batchChannel.Writer.TryWrite((subject, exactData));
        }

        // Khử async void, thay bằng void và khởi chạy Task

        public void OnMessage<T>(string topic, Action<(string regionId, T data)> callback)
            where T : IMessage, new()
        {
            var subject = NatifyTopics.GetServerListenSubject(_serverName, _clientNameToConnect, topic);

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in _connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName,
                                       cancellationToken: _cts.Token))
                    {
                        var regionId = NatifyTopics.ExtractRegionIdFromServerSubject(msg.Subject);

                        // 1. Kiểm tra MessageId và Xử lý ACK (Cho nguyên cả 1 Batch)
                        msg.Headers?.TryGetValue("Natify-MsgId", out var messageId);

                        if (!string.IsNullOrEmpty(messageId))
                        {
                            string ackSubject =
                                $"NatifyClient.{_clientNameToConnect}.{_serverName}.{regionId}.ACK.{messageId}";
                            _ = _connection.PublishAsync(ackSubject, Array.Empty<byte>()).AsTask();

                            // 2. Chống trùng lặp toàn bộ Batch
                            if (_processedMessages.TryAdd(messageId, 1))
                            {
                                // BÁO CÁO: Tăng CurrentCache
                                Trigger.AddDedupItem();

                                // Thêm vào Time Wheel để 10 giây sau tự hủy
                                long expireTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10_000;
                                _messageTtlWheel.AddOrUpdate(messageId, 1, expireTimeMs);
                            }
                            else
                            {
                                // Đã có trong cache -> Skip
                                continue;
                            }
                        }

                        // 3. GIẢI NÉN BATCH
                        try
                        {
                            var payload = msg.Data ?? Array.Empty<byte>();

                            Trigger.AddBatchReceived();

                            // Parse ra đối tượng NatifyBatch trước
                            var batch = NatifySerializer.Deserialize<NatifyBatch>(payload, payload.Length);

                            Trigger.AddReceived(payload.Length, batch.Payloads.Count);

                            // 4. Lặp qua từng tin nhắn nhỏ bên trong và gọi Callback
                            foreach (var byteString in batch.Payloads)
                            {
                                // Chuyển ByteString về mảng byte[] (Lưu ý: Protobuf C# hỗ trợ ToByteArray)
                                byte[] itemBytes = byteString.ToByteArray();

                                // Chuyển byte[] thành kiểu T của User
                                var data = NatifySerializer.Deserialize<T>(itemBytes, itemBytes.Length);

                                // Gọi logic xử lý của Game
                                callback((regionId, data));
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[NatifyServer] Error Unpacking Batch on {topic}: {ex.Message}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
            });
        }


        public async Task<TRes> RequestAsync<TReq, TRes>(string topic, string regionId, TReq requestData,
            TimeSpan timeout)
            where TReq : IMessage
            where TRes : IMessage, new()
        {
            if (_isDisposed) throw new ObjectDisposedException(nameof(NatifyServer));

            var subject = NatifyTopics.GetServerPublishSubject(_clientNameToConnect, _serverName, regionId, topic);
            var (reqBuffer, reqLength) = NatifySerializer.Serialize(requestData);

            var exactData = new byte[reqLength];
            Array.Copy(reqBuffer, exactData, reqLength);
            System.Buffers.ArrayPool<byte>.Shared.Return(reqBuffer);

            try
            {
                var reply = await _connection.RequestAsync<byte[], byte[]>(
                    subject, exactData,
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

        /// <summary>
        /// Xử lý Request đồng bộ (CPU-bound / Tính toán nhanh)
        /// </summary>
        public void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq request), TRep> handler)
            where TReq : IMessage, new()
            where TRep : IMessage
        {
            var subject = NatifyTopics.GetServerListenSubject(_serverName, _clientNameToConnect, topic);

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in _connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName,
                                       cancellationToken: _cts.Token))
                    {
                        // Nếu tin nhắn không có hộp thư trả lời (ReplyTo), bỏ qua vì đây không phải là Request
                        if (string.IsNullOrEmpty(msg.ReplyTo)) continue;

                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                // 1. Trích xuất Region và Giải mã Request (An toàn với 0 byte Protobuf)
                                var regionId = NatifyTopics.ExtractRegionIdFromServerSubject(msg.Subject);
                                var payload = msg.Data ?? Array.Empty<byte>();
                                var requestData = NatifySerializer.Deserialize<TReq>(payload, payload.Length);

                                // 2. Chạy logic xử lý của Server (Đồng bộ)
                                TRep replyData = handler((regionId, requestData));

                                // 3. Mã hóa kết quả trả về
                                var (replyBuffer, replyLength) = NatifySerializer.Serialize(replyData);
                                var exactReplyData = new byte[replyLength];
                                Array.Copy(replyBuffer, exactReplyData, replyLength);
                                System.Buffers.ArrayPool<byte>.Shared.Return(replyBuffer);

                                // 4. Bắn kết quả ngược lại đúng hòm thư INBOX của Client
                                await _connection.PublishAsync(msg.ReplyTo, exactReplyData,
                                    cancellationToken: _cts.Token);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[NatifyServer] OnRequest (Sync) Error on {topic}: {ex.Message}");
                            }
                        });
                    }
                }
                catch (OperationCanceledException)
                {
                    /* Server đang tắt */
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[NatifyServer] Subscribe Request Error on {topic}: {ex.Message}");
                }
            });
        }

        /// <summary>
        /// Xử lý Request bất đồng bộ (I/O-bound / Gọi Database, API ngoài)
        /// </summary>
        public void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq request), Task<TRep>> handlerAsync)
            where TReq : IMessage, new()
            where TRep : IMessage
        {
            var subject = NatifyTopics.GetServerListenSubject(_serverName, _clientNameToConnect, topic);

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in _connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName,
                                       cancellationToken: _cts.Token))
                    {
                        if (string.IsNullOrEmpty(msg.ReplyTo)) continue;

                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                // 1. Giải mã
                                var regionId = NatifyTopics.ExtractRegionIdFromServerSubject(msg.Subject);
                                var payload = msg.Data ?? Array.Empty<byte>();
                                var requestData = NatifySerializer.Deserialize<TReq>(payload, payload.Length);

                                // 2. Chạy logic xử lý của Server (Bất đồng bộ với await)
                                TRep replyData = await handlerAsync((regionId, requestData));

                                // 3. Mã hóa
                                var (replyBuffer, replyLength) = NatifySerializer.Serialize(replyData);
                                var exactReplyData = new byte[replyLength];
                                Array.Copy(replyBuffer, exactReplyData, replyLength);
                                System.Buffers.ArrayPool<byte>.Shared.Return(replyBuffer);

                                // 4. Trả lời Client
                                await _connection.PublishAsync(msg.ReplyTo, exactReplyData,
                                    cancellationToken: _cts.Token);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[NatifyServer] OnRequest (Async) Error on {topic}: {ex.Message}");
                            }
                        });
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[NatifyServer] Subscribe Request Error on {topic}: {ex.Message}");
                }
            });
        }

        // Thêm hàm BatchWorkerAsync vào NatifyServer:
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
                    if (elapsed >= MaxWait) break;

                    if (reader.TryRead(out var item))
                    {
                        if (!batches.TryGetValue(item.Subject, out var batch))
                        {
                            batch = new NatifyBatch();
                            batches[item.Subject] = batch;
                        }

                        batch.Payloads.Add(ByteString.CopyFrom(item.Payload));
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
                        string messageId = Guid.NewGuid().ToString("N");

                        var (buffer, length) = NatifySerializer.Serialize(batchMsg);
                        var exactData = new byte[length];
                        Array.Copy(buffer, exactData, length);
                        System.Buffers.ArrayPool<byte>.Shared.Return(buffer);

                        // Ghi nhận tổng số tin nhắn lẻ
                        Trigger.AddSent(length, batchMsg.Payloads.Count);

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

        public void Dispose()
        {
            if (_isDisposed) return;
            _isDisposed = true;

            // Khóa van Phễu (Thêm 2 dòng này)
            _batchChannel.Writer.Complete();
            if (_batchWorkerTask != null) { try { _batchWorkerTask.Wait(TimeSpan.FromSeconds(2)); } catch { } }

            // BƯỚC 1: Chờ đợi "Xả hàng" (Drain)
            // Chúng ta đợi tối đa 2 giây để:
            // - Client kịp gửi nốt ACK cho các gói tin Server vừa gửi.
            // - Luồng OnMessage xử lý xong nốt Batch đang giải nén.

            var waitStartTime = DateTime.UtcNow;
            while ((!_unackedMessages.IsEmpty) && (DateTime.UtcNow - waitStartTime).TotalSeconds < 2)
            {
                // Tạm nghỉ một chút để luồng ACK Listener kịp làm việc
                Thread.Sleep(50);
            }

            // BƯỚC 2: Sập cầu dao (Cancel Token)
            // Lúc này mới dừng các luồng lặp vĩnh viễn
            try
            {
                _cts.Cancel();
            }
            catch
            {
            }

            // BƯỚC 3: Đợi các Task chạy ngầm kết thúc hoàn toàn
            try
            {
                Task.WaitAll(new[] { _retryWorkerTask, _ackListenerTask }, TimeSpan.FromSeconds(1));
            }
            catch
            {
            }

            // BƯỚC 4: Giải phóng tài nguyên hệ thống
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