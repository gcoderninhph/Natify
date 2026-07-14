using System;
using System.Collections.Generic;

namespace Natify
{
    internal sealed class BatchAccumulator : IDisposable
    {
        private readonly List<RentedBuffer> _payloads = new();
        private readonly List<string> _reqIds = new();
        private readonly List<string> _msgTypes = new();
        private readonly List<string> _repIds = new();

        public int Count => _payloads.Count;
        public int TotalPayloadBytes { get; private set; }

        public IReadOnlyList<RentedBuffer> Payloads => _payloads;
        public IReadOnlyList<string> ReqIds => _reqIds;
        public IReadOnlyList<string> MsgTypes => _msgTypes;
        public IReadOnlyList<string> RepIds => _repIds;

        public void Add(RentedBuffer payload, string reqId, string msgType, string repId)
        {
            _payloads.Add(payload);
            _reqIds.Add(reqId);
            _msgTypes.Add(msgType);
            _repIds.Add(repId);
            TotalPayloadBytes += payload.Length;
        }

        public void Dispose()
        {
            foreach (var p in _payloads)
                p.Dispose();
            _payloads.Clear();
            _reqIds.Clear();
            _msgTypes.Clear();
            _repIds.Clear();
        }
    }
}
