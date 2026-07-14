using System;
using System.Buffers;
using Google.Protobuf;

namespace Natify
{
    public static class NatifySerializer
    {
        public static byte[] SerializeSimple<T>(T message) where T : IMessage
        {
            int size = message.CalculateSize();
            byte[] data = new byte[size];
            using var stream = new CodedOutputStream(data);
            message.WriteTo(stream);
            return data;
        }

        public static RentedBuffer SerializePooled<T>(T message) where T : IMessage
        {
            int size = message.CalculateSize();
            byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
            using var stream = new CodedOutputStream(buffer);
            message.WriteTo(stream);
            return new RentedBuffer(buffer, size);
        }

        public static T Deserialize<T>(byte[] data, int length) where T : IMessage, new()
        {
            var message = new T();
            message.MergeFrom(data, 0, length);
            return message;
        }
    }
}