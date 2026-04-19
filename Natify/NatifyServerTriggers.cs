using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Natify
{
    public class NatifyServerTriggers : IDisposable
    {
        // 1. CÁC BIẾN LƯU TRỮ (Sử dụng long để tránh tràn số khi chạy lâu dài)
        private long _bytesSent;
        private long _bytesReceived;
        private long _messagesSent;
        private long _messagesReceived;
        private long _batchesReceived;
        private long _errorsCount;
        private long _currentDedupCacheSize;
        private long _totalDedupExpired;

        // 2. PUBLIC PROPERTIES (Để User có thể get ra bất cứ lúc nào một cách an toàn)
        public long BytesSent => Interlocked.Read(ref _bytesSent);
        public long BytesReceived => Interlocked.Read(ref _bytesReceived);
        public long MessagesSent => Interlocked.Read(ref _messagesSent);
        public long MessagesReceived => Interlocked.Read(ref _messagesReceived);
        public long BatchesReceived => Interlocked.Read(ref _batchesReceived);

        public long ErrorsCount => Interlocked.Read(ref _errorsCount);

        // 1. Số lượng gói tin ĐANG nằm trong RAM để chờ đối chiếu trùng lặp
        public long CurrentDedupCacheSize => Interlocked.Read(ref _currentDedupCacheSize);

        // 2. Số lượng gói tin ĐÃ ĐƯỢC GIẢI PHÓNG khỏi RAM bởi TimeWheel
        public long TotalDedupExpired => Interlocked.Read(ref _totalDedupExpired);

        // 3. ĐỌC TRỰC TIẾP RAM VẬT LÝ của Server (Đơn vị: Megabytes)
        public double ProcessMemoryMB => Process.GetCurrentProcess().WorkingSet64 / (1024.0 * 1024.0);


        // 3. INTERNAL METHODS (Chỉ cho phép nội bộ NatifyServer gọi để cộng dồn)
        internal void AddSent(int bytes, int msgCount = 1)
        {
            Interlocked.Add(ref _bytesSent, bytes);
            Interlocked.Add(ref _messagesSent, msgCount);
        }

        internal void AddReceived(int bytes, int msgCount = 1)
        {
            Interlocked.Add(ref _bytesReceived, bytes);
            Interlocked.Add(ref _messagesReceived, msgCount);
        }

        internal void AddBatchReceived() => Interlocked.Increment(ref _batchesReceived);
        internal void AddError() => Interlocked.Increment(ref _errorsCount);
        internal void AddDedupItem() => Interlocked.Increment(ref _currentDedupCacheSize);

        internal void RemoveDedupItems(int count)
        {
            Interlocked.Add(ref _currentDedupCacheSize, -count); // Trừ đi số ID vừa xóa
            Interlocked.Add(ref _totalDedupExpired, count); // Cộng vào số ID đã giải phóng
        }


        // ==========================================
        // HỆ THỐNG ĐÁNH GIÁ TRIGGER THÔNG MINH
        // ==========================================

        // Cấu trúc lưu trữ 1 Rule (Luật kích hoạt)
        private class TriggerRule
        {
            public Func<NatifyServerTriggers, bool> Condition { get; set; }
            public Action<NatifyServerTriggers> Callback { get; set; }
            public bool IsOneTime { get; set; } // Nếu true, kích hoạt xong sẽ tự xóa
        }

        private readonly ConcurrentDictionary<Guid, TriggerRule> _rules = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly int _monitorIntervalMs = 500; // Quét mỗi 500ms

        public NatifyServerTriggers()
        {
            // Khởi động luồng ngầm chuyên làm nhiệm vụ check Trigger
            _ = Task.Run(MonitorLoopAsync);
        }

        /// <summary>
        /// Đăng ký một Trigger tùy biến.
        /// </summary>
        /// <param name="condition">Điều kiện để kích hoạt (Ví dụ: t => t.BytesReceived > 1024)</param>
        /// <param name="action">Hành động khi kích hoạt</param>
        /// <param name="oneTime">Chỉ kích hoạt 1 lần duy nhất rồi hủy?</param>
        /// <returns>ID của rule để có thể hủy sau này</returns>
        public Guid RegisterTrigger(Func<NatifyServerTriggers, bool> condition, Action<NatifyServerTriggers> action,
            bool oneTime = false)
        {
            var ruleId = Guid.NewGuid();
            var rule = new TriggerRule { Condition = condition, Callback = action, IsOneTime = oneTime };
            _rules.TryAdd(ruleId, rule);
            return ruleId;
        }

        public void RemoveTrigger(Guid ruleId)
        {
            _rules.TryRemove(ruleId, out _);
        }

        private async Task MonitorLoopAsync()
        {
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_monitorIntervalMs, _cts.Token);

                    // Duyệt qua tất cả các luật xem có cái nào thỏa mãn không
                    foreach (var kvp in _rules)
                    {
                        var ruleId = kvp.Key;
                        var rule = kvp.Value;

                        if (rule.Condition(this))
                        {
                            // Nếu thỏa mãn -> Chạy callback trên một luồng khác để không block hệ thống
                            Task.Run(() => rule.Callback(this));

                            // Nếu là loại kích hoạt 1 lần thì xóa nó đi
                            if (rule.IsOneTime)
                            {
                                _rules.TryRemove(ruleId, out _);
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception)
                {
                    /* Ghi log lỗi ngầm nếu cần */
                }
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            _cts.Dispose();
        }
    }
}