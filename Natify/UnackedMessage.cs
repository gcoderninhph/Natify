using System;

namespace Natify
{
    public class UnackedMessage
    {
        public string Subject { get; set; }
        public byte[] Payload { get; set; }
        public string BatchId { get; set; }
        public DateTime LastSent { get; set; }
        public int RetryCount { get; set; }
    }

    public readonly struct Data<T>
    {
        public readonly T Value;
        public readonly string InstanceId;
        public readonly string ReqId;
        public readonly string RepId;


        public Data(T value, string instanceId, string reqId, string repId)
        {
            Value = value;
            InstanceId = instanceId;
            ReqId = reqId;
            RepId = repId;
        }
    }
}