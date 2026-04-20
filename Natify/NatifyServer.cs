using System;
using System.Buffers;
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
    public class NatifyServer : IDisposable
    {
        private readonly string _instanceId;

        private readonly ConcurrentDictionary<string, byte> _processedMessages = new();
        private readonly TimedSortedSet<string, byte> _messageTtlWheel = new();

        private readonly Channel<(string Subject, byte[] Payload, string MessageType, string ReqId, string RepId)>
            _batchChannel =
                Channel.CreateUnbounded<(string, byte[], string, string, string)>();

        private readonly ConcurrentDictionary<string, (TaskCompletionSource<byte[]> task, CancellationTokenSource ct)>
            _replyTasks = new();

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

        private Task? _retryWorkerTask;
        private Task? _ackListenerTask;

        public NatifyServer(string url, string serverName, string groupName, string clientNameToConnect)
        {
            _instanceId = Guid.NewGuid().ToString();
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
            OnMessageRep();
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
                                        $"[NatifyServer] Drop gói tin {unacked.BatchId} gửi tới Client vì vượt quá Retry.");
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
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            });
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
                    if (elapsed >= MaxWait) break;

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
                        ArrayPool<byte>.Shared.Return(buffer);

                        // Ghi nhận tổng số tin nhắn lẻ
                        Trigger.AddSent(length, batchMsg.Payloads.Count);

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

        public void Publish<T>(string topic, string regionId, T message) where T : IMessage =>
            Publish(topic, regionId, message, "PUB", out _, string.Empty);

        private void Publish<T>(string topic, string regionId, T message, string messageType, out string reqId,
            string repId)
            where T : IMessage
        {
            reqId = string.Empty;
            if (_isDisposed) return;

            var subject = NatifyTopics.GetServerPublishSubject(_clientNameToConnect, _serverName, regionId, topic);

            var (buffer, length) = NatifySerializer.Serialize(message);
            var exactData = new byte[length];
            Array.Copy(buffer, exactData, length);
            ArrayPool<byte>.Shared.Return(buffer);

            reqId = Guid.NewGuid().ToString("N");

            // Bắn vào Phễu
            _batchChannel.Writer.TryWrite((subject, exactData, messageType, reqId, repId));
        }

        private void OnMessageRep()
        {
            OnMessage($"Rep-{_instanceId}", a =>
            {
                if (_replyTasks.TryRemove(a.data.RepId, out var task))
                {
                    task.task.SetResult(a.data.Value);
                    task.ct.Dispose();
                }
            }, null);
        }

        public void OnMessage<T>(string topic, Action<(string regionId, Data<T> data)> callback)
            where T : IMessage, new()
        {
            OnMessage(topic, a =>
            {
                var result = NatifySerializer.Deserialize<T>(a.data.Value, a.data.Value.Length);
                callback((a.regionId, new Data<T>(result, a.data.InstanceId, a.data.ReqId, a.data.RepId)));
            }, null);
        }

        public void OnMessage<T>(string topic, Func<(string regionId, Data<T> data), Task> callback)
            where T : IMessage, new()
        {
            OnMessage(topic, null, async a =>
            {
                var result = NatifySerializer.Deserialize<T>(a.data.Value, a.data.Value.Length);
                await callback((a.regionId, new Data<T>(result, a.data.InstanceId, a.data.ReqId, a.data.RepId)));
            });
        }


        private void OnMessage(string topic, Action<(string regionId, Data<byte[]> data)>? callback,
            Func<(string regionId, Data<byte[]> data), Task>? callbackAsync)

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
                        string messageId = string.Empty;
                        if (msg.Headers != null && msg.Headers.TryGetValue("Natify-BatchId", out var msgIdVal))
                        {
                            messageId = msgIdVal.ToString();
                        }

                        if (!string.IsNullOrEmpty(messageId))
                        {
                            string ackSubject =
                                $"NatifyClient.{_clientNameToConnect}.{_serverName}.{regionId}.ACK.{messageId}";
                            _ = _connection.PublishAsync(ackSubject, Array.Empty<byte>()).AsTask();

                            if (_processedMessages.TryAdd(messageId, 1))
                            {
                                Trigger.AddDedupItem();
                                long expireTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10_000;
                                _messageTtlWheel.AddOrUpdate(messageId, 1, expireTimeMs);
                            }
                            else
                            {
                                continue; // Bỏ qua trùng lặp
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
                            for (var i = 0; i < batch.Payloads.Count; i++)
                            {
                                var itemBytes = batch.Payloads[i].ToByteArray();
                                var instanceId = batch.FormInstanceId;
                                var reqId = batch.ReqId[i];
                                var repId = batch.RepId[i];
                                // var data = NatifySerializer.Deserialize<T>(itemBytes, itemBytes.Length);
                                // callback.Invoke(new Data<T>(data, instanceId, repId));
                                var result = new Data<byte[]>(itemBytes, instanceId, reqId, repId);
                                callback?.Invoke((regionId, result));
                                if (callbackAsync != null) _ = callbackAsync((regionId, result));
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
            Publish(topic, regionId, requestData, "REQ", out var reqId, string.Empty);

            if (!string.IsNullOrEmpty(reqId))
            {
                var cancellationTokenSource = new CancellationTokenSource();
                var taskCompletionSource = new TaskCompletionSource<byte[]>();

                // --- BẮT ĐẦU ĐOẠN CODE CẦN THÊM ---
                // Lắng nghe sự kiện token bị hủy (hết giờ)
                cancellationTokenSource.Token.Register(() =>
                {
                    // Xóa task khỏi danh sách chờ để dọn dẹp RAM
                    if (_replyTasks.TryRemove(reqId, out _))
                    {
                        // Ép Task phải ném ra lỗi TimeoutException ngay lập tức
                        taskCompletionSource.TrySetException(
                            new TimeoutException(
                                $"[Natify] Request {reqId} timed out after {timeout.TotalMilliseconds}ms."));
                    }
                });
                // --- KẾT THÚC ĐOẠN CODE CẦN THÊM ---

                cancellationTokenSource.CancelAfter(timeout);
                _replyTasks[reqId] = (taskCompletionSource, cancellationTokenSource);

                // Lúc này, nếu quá 1 giây, TrySetException ở trên sẽ được kích hoạt
                // Lệnh await dưới đây sẽ lập tức văng ra TimeoutException
                var result = await taskCompletionSource.Task;

                var t = NatifySerializer.Deserialize<TRes>(result, result.Length);
                return t;
            }

            throw new Exception($"[NatifyClient] Request Failed: {reqId}");
        }

        /// <summary>
        /// Xử lý Request đồng bộ (CPU-bound / Tính toán nhanh)
        /// </summary>
        public void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq request), TRep> handler)
            where TReq : IMessage, new()
            where TRep : IMessage
        {
            OnMessage<TReq>(topic, tReq =>
            {
                var result = handler((tReq.regionId, tReq.data.Value));
                Publish($"Rep-{tReq.data.InstanceId}", tReq.regionId, result, "REP", out var reqId, tReq.data.ReqId);
            });
        }

        /// <summary>
        /// Xử lý Request bất đồng bộ (I/O-bound / Gọi Database, API ngoài)
        /// </summary>
        public void OnRequest<TReq, TRep>(string topic, Func<(string regionId, TReq request), Task<TRep>> handlerAsync)
            where TReq : IMessage, new()
            where TRep : IMessage
        {
            OnMessage<TReq>(topic, async tReq =>
            {
                var result = await handlerAsync((tReq.regionId, tReq.data.Value));
                Publish($"Rep-{tReq.data.InstanceId}", tReq.regionId, result, "REP", out var reqId, tReq.data.ReqId);
            });
        }

        public void Dispose()
        {
            if (_isDisposed) return;
            _isDisposed = true;

            // Khóa van Phễu (Thêm 2 dòng này)
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