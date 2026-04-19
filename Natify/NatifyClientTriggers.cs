using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Natify
{
    public class NatifyClientTriggers : IDisposable
    {
        // --- CÁC BIẾN LƯU TRỮ ---
        private long _bytesSent;
        private long _bytesReceived;
        private long _messagesSent;
        private long _messagesReceived;
        private long _batchesSent; // Client gửi Batch lên Server
        private long _errorsCount;

        // --- CÁC BIẾN KIỂM SOÁT RAM & CACHE ---
        private long _currentDedupCacheSize; 
        private long _totalDedupExpired;

        public long BytesSent => Interlocked.Read(ref _bytesSent);
        public long BytesReceived => Interlocked.Read(ref _bytesReceived);
        public long MessagesSent => Interlocked.Read(ref _messagesSent);
        public long MessagesReceived => Interlocked.Read(ref _messagesReceived);
        public long BatchesSent => Interlocked.Read(ref _batchesSent);
        public long ErrorsCount => Interlocked.Read(ref _errorsCount);

        public long CurrentDedupCacheSize => Interlocked.Read(ref _currentDedupCacheSize);
        public long TotalDedupExpired => Interlocked.Read(ref _totalDedupExpired);

        public double ProcessMemoryMB => Process.GetCurrentProcess().WorkingSet64 / (1024.0 * 1024.0);

        // --- INTERNAL METHODS ---
        internal void AddSent(int bytes, int msgCount = 1) { Interlocked.Add(ref _bytesSent, bytes); Interlocked.Add(ref _messagesSent, msgCount); }
        internal void AddReceived(int bytes, int msgCount = 1) { Interlocked.Add(ref _bytesReceived, bytes); Interlocked.Add(ref _messagesReceived, msgCount); }
        internal void AddBatchSent() => Interlocked.Increment(ref _batchesSent);
        internal void AddError() => Interlocked.Increment(ref _errorsCount);

        internal void AddDedupItem() => Interlocked.Increment(ref _currentDedupCacheSize);
        internal void RemoveDedupItems(int count)
        {
            Interlocked.Add(ref _currentDedupCacheSize, -count);
            Interlocked.Add(ref _totalDedupExpired, count);
        }

        // --- HỆ THỐNG TRIGGER THÔNG MINH ---
        private class TriggerRule
        {
            public Func<NatifyClientTriggers, bool> Condition { get; set; }
            public Action<NatifyClientTriggers> Callback { get; set; }
            public bool IsOneTime { get; set; }
        }

        private readonly ConcurrentDictionary<Guid, TriggerRule> _rules = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly int _monitorIntervalMs = 500;

        public NatifyClientTriggers()
        {
            _ = Task.Run(MonitorLoopAsync);
        }

        public Guid RegisterTrigger(Func<NatifyClientTriggers, bool> condition, Action<NatifyClientTriggers> action, bool oneTime = false)
        {
            var ruleId = Guid.NewGuid();
            var rule = new TriggerRule { Condition = condition, Callback = action, IsOneTime = oneTime };
            _rules.TryAdd(ruleId, rule);
            return ruleId;
        }

        public void RemoveTrigger(Guid ruleId) => _rules.TryRemove(ruleId, out _);

        private async Task MonitorLoopAsync()
        {
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_monitorIntervalMs, _cts.Token);
                    foreach (var kvp in _rules)
                    {
                        var rule = kvp.Value;
                        if (rule.Condition(this))
                        {
                            Task.Run(() => rule.Callback(this));
                            if (rule.IsOneTime) _rules.TryRemove(kvp.Key, out _);
                        }
                    }
                }
                catch (OperationCanceledException) { break; }
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            _cts.Dispose();
        }
    }
}