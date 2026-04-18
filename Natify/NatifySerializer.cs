using System;
using System.Buffers;
using Google.Protobuf;

namespace Natify
{
    public static class NatifySerializer
    {
        public static (byte[] Buffer, int Length) Serialize<T>(T message) where T : IMessage
        {
            int size = message.CalculateSize();
            byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
            
            using (var stream = new CodedOutputStream(buffer))
            {
                message.WriteTo(stream);
            }
            
            return (buffer, size);
        }

        public static T Deserialize<T>(byte[] data, int length) where T : IMessage, new()
        {
            var message = new T();
            message.MergeFrom(data, 0, length);
            return message;
        }
    }
}