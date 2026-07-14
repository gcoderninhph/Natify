using System;
using System.Buffers;
using System.Text;
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

        public static T Deserialize<T>(ByteString data) where T : IMessage, new()
        {
            var message = new T();
            message.MergeFrom(data);
            return message;
        }

        public static RentedBuffer SerializeBatchPooled(
            IReadOnlyList<RentedBuffer> payloads,
            IReadOnlyList<string> reqIds,
            IReadOnlyList<string> msgTypes,
            IReadOnlyList<string> repIds,
            string fromInstanceId)
        {
            int n = payloads.Count;

            int totalSize = 0;
            for (int i = 0; i < n; i++)
            {
                int plen = payloads[i].Length;
                totalSize += 1 + VarintSize(plen) + plen;

                int rlen = Encoding.UTF8.GetByteCount(reqIds[i]);
                totalSize += 1 + VarintSize(rlen) + rlen;

                int mlen = Encoding.UTF8.GetByteCount(msgTypes[i]);
                totalSize += 1 + VarintSize(mlen) + mlen;

                int elen = Encoding.UTF8.GetByteCount(repIds[i]);
                totalSize += 1 + VarintSize(elen) + elen;
            }

            int fromLen = Encoding.UTF8.GetByteCount(fromInstanceId);
            if (fromLen > 0)
                totalSize += 1 + VarintSize(fromLen) + fromLen;

            byte[] buffer = ArrayPool<byte>.Shared.Rent(totalSize);
            int pos = 0;

            for (int i = 0; i < n; i++)
            {
                buffer[pos++] = 0x0A;
                pos += WriteVarint(buffer, pos, (ulong)payloads[i].Length);
                payloads[i].Data.Span.CopyTo(buffer.AsSpan(pos));
                pos += payloads[i].Length;

                buffer[pos++] = 0x12;
                int rlen = Encoding.UTF8.GetByteCount(reqIds[i]);
                pos += WriteVarint(buffer, pos, (ulong)rlen);
                pos += Encoding.UTF8.GetBytes(reqIds[i], buffer.AsSpan(pos));

                buffer[pos++] = 0x1A;
                int mlen = Encoding.UTF8.GetByteCount(msgTypes[i]);
                pos += WriteVarint(buffer, pos, (ulong)mlen);
                pos += Encoding.UTF8.GetBytes(msgTypes[i], buffer.AsSpan(pos));

                buffer[pos++] = 0x22;
                int elen = Encoding.UTF8.GetByteCount(repIds[i]);
                pos += WriteVarint(buffer, pos, (ulong)elen);
                pos += Encoding.UTF8.GetBytes(repIds[i], buffer.AsSpan(pos));
            }

            if (fromLen > 0)
            {
                buffer[pos++] = 0x2A;
                pos += WriteVarint(buffer, pos, (ulong)fromLen);
                pos += Encoding.UTF8.GetBytes(fromInstanceId, buffer.AsSpan(pos));
            }

            return new RentedBuffer(buffer, totalSize);
        }

        private static int VarintSize(int value) => VarintSize((ulong)value);

        private static int VarintSize(ulong value)
        {
            int size = 1;
            while (value >= 128) { size++; value >>= 7; }
            return size;
        }

        private static int WriteVarint(byte[] buffer, int pos, ulong value)
        {
            int start = pos;
            while (value >= 128)
            {
                buffer[pos++] = (byte)(value | 0x80);
                value >>= 7;
            }
            buffer[pos++] = (byte)value;
            return pos - start;
        }
    }
}