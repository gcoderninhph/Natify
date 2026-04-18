using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Google.Protobuf;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

#if UNITY_5_3_OR_NEWER
using UnityEngine;
#endif

namespace Natify
{
    public class NatifyClient : IDisposable
    {
        private readonly INatsConnection _connection;
        private readonly INatsJSContext _jsContext;
        private readonly string _clientName;
        private readonly string _groupName;
        private readonly string _regionId;
        private readonly string _serverNameToConnect;
        private bool _isDisposed = false;

        private readonly Channel<(string Subject, byte[] Data, int Length)> _publishChannel;
        private readonly CancellationTokenSource _cts;
        private readonly ConcurrentQueue<Action> _mainThreadActions = new ConcurrentQueue<Action>();

        public NatifyClient(string url, string clientName, string groupName, string regionId,
            string serverNameToConnect)
        {
            _clientName = clientName;
            _groupName = groupName;
            _regionId = regionId;
            _serverNameToConnect = serverNameToConnect;

            var opts = new NatsOpts
            {
                Url = url
            };
            _connection = new NatsConnection(opts);

            // Dùng GetAwaiter().GetResult() an toàn hơn Wait()
            _connection.ConnectAsync().AsTask().GetAwaiter().GetResult();

            _cts = new CancellationTokenSource();
            _publishChannel = Channel.CreateUnbounded<(string, byte[], int)>();
            _jsContext = new NatsJSContext(_connection);

            // Khởi chạy luồng ngầm an toàn (thay thế cho Thread truyền thống gây cảnh báo)
            _ = Task.Run(PublishWorkerAsync, _cts.Token);
        }

        public void Publish<T>(string topic, T message) where T : IMessage
        {
            if (_isDisposed) return; // Không cho phép gửi nếu Client đã tắt

            var subject = NatifyTopics.GetClientPublishSubject(_serverNameToConnect, _clientName, _regionId, topic);
            var (buffer, length) = NatifySerializer.Serialize(message);
            _publishChannel.Writer.TryWrite((subject, buffer, length));
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
                        // KHÔNG DÙNG: if (msg.Data == null) continue;

                        _mainThreadActions.Enqueue(() =>
                        {
                            try
                            {
                                // Xử lý an toàn tin nhắn 0 byte
                                var payload = msg.Data ?? Array.Empty<byte>();
                                var data = NatifySerializer.Deserialize<T>(payload, payload.Length);

                                callback(data);
                            }
                            catch (Exception ex)
                            {
                                LogCallbackException(topic, ex);
                            }
                        });
                    }
                }
                catch (OperationCanceledException)
                {
                    /* Bỏ qua khi Dispose */
                }
                catch (Exception ex)
                {
                    LogError($"[Natify] Subscribe Error on {topic}: {ex.Message}");
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

        private async Task PublishWorkerAsync()
        {
            var reader = _publishChannel.Reader;
            var batch = new System.Collections.Generic.List<(string Subject, byte[] Data, int Length)>(50);

            try
            {
                while (await reader.WaitToReadAsync(_cts.Token))
                {
                    // 1. Rút nhanh nhất có thể các tin nhắn hiện có (Tối đa 50)
                    while (batch.Count < 50 && reader.TryRead(out var msg))
                    {
                        batch.Add(msg);
                    }

                    // 2. Nếu chưa đủ 50 tin, chờ 10ms xem có ai gửi thêm không (Dùng Delay siêu an toàn)
                    if (batch.Count > 0 && batch.Count < 50)
                    {
                        try
                        {
                            await Task.Delay(10, _cts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                        }

                        // Rút nốt những tin nhắn mới bay vào trong 10ms vừa qua
                        while (batch.Count < 50 && reader.TryRead(out var moreMsg))
                        {
                            batch.Add(moreMsg);
                        }
                    }

                    // 3. Xả Batch xuống NATS
                    if (batch.Count > 0)
                    {
                        foreach (var m in batch)
                        {
                            var exactData = new byte[m.Length];
                            Array.Copy(m.Data, exactData, m.Length);
                            System.Buffers.ArrayPool<byte>.Shared.Return(m.Data);

                            // [QUAN TRỌNG NHẤT]: Phải có await ở đây để NATS đẩy TCP tuần tự, không tràn buffer
                            await _connection.PublishAsync(m.Subject, exactData, cancellationToken: _cts.Token);
                        }

                        batch.Clear();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                /* Bỏ qua khi tắt game/server */
            }
            catch (Exception ex)
            {
                LogError($"[Natify] PublishWorker failed: {ex.Message}");
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
            // Cờ bảo vệ: Đã Dispose rồi thì không làm gì nữa
            if (_isDisposed) return;
            _isDisposed = true;

            try
            {
                _cts.Cancel();
            }
            catch
            {
                /* Bỏ qua lỗi nếu Token đã bị huỷ */
            }

            _publishChannel.Writer.Complete();

            try
            {
                _connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch
            {
            }

            _cts.Dispose();
        }
    }
}