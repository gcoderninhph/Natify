using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using NATS.Client.Core;

namespace Natify
{
    public class NatifyServer : IDisposable
    {
        private readonly INatsConnection _connection;
        private readonly string _serverName;
        private readonly string _groupName;
        private readonly string _clientNameToConnect;
        private readonly CancellationTokenSource _cts;
        private bool _isDisposed = false;

        public NatifyServer(string url, string serverName, string groupName, string clientNameToConnect)
        {
            _serverName = serverName;
            _groupName = groupName;
            _clientNameToConnect = clientNameToConnect;

            var opts = new NatsOpts { Url = url };
            _connection = new NatsConnection(opts);

            _connection.ConnectAsync().AsTask().GetAwaiter().GetResult();
            _cts = new CancellationTokenSource();
        }

        public void Publish<T>(string topic, string regionId, T message) where T : IMessage
        {
            var subject = NatifyTopics.GetServerPublishSubject(_clientNameToConnect, _serverName, regionId, topic);
            var (buffer, length) = NatifySerializer.Serialize(message);

            var exactData = new byte[length];
            Array.Copy(buffer, exactData, length);
            ArrayPool<byte>.Shared.Return(buffer);

            // Dùng discard variable '_' để khử cảnh báo CS4014
            _ = _connection.PublishAsync(subject, exactData).AsTask();
        }

        // Khử async void, thay bằng void và khởi chạy Task
        public void OnMessage<T>(string topic, Action<(string regionId, T data)> callback) where T : IMessage, new()
        {
            var subject = NatifyTopics.GetServerListenSubject(_serverName, _clientNameToConnect, topic);

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in _connection.SubscribeAsync<byte[]>(subject, queueGroup: _groupName,
                                       cancellationToken: _cts.Token))
                    {
                        // KHÔNG DÙNG: if (msg.Data == null) continue;

                        try
                        {
                            var regionId = NatifyTopics.ExtractRegionIdFromServerSubject(msg.Subject);

                            // Xử lý an toàn tin nhắn 0 byte của Protobuf
                            var payload = msg.Data ?? Array.Empty<byte>();
                            var data = NatifySerializer.Deserialize<T>(payload, payload.Length);

                            callback((regionId, data));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[NatifyServer] Callback Error on topic {topic}: {ex.Message}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    /* Bỏ qua khi server đóng */
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[NatifyServer] Subscribe Error on {topic}: {ex.Message}");
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

        public void Dispose()
        {
            _cts.Cancel();
            _connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _cts.Dispose();
            _isDisposed = true;
        }
    }
}