using System;
using System.Buffers;

namespace Natify
{
    public sealed class RentedBuffer : IDisposable
    {
        private byte[]? _buffer;
        private readonly int _length;

        public ReadOnlyMemory<byte> Data
        {
            get
            {
                if (_buffer == null)
                    throw new ObjectDisposedException(nameof(RentedBuffer));
                return new ReadOnlyMemory<byte>(_buffer, 0, _length);
            }
        }

        public int Length => _length;

        internal RentedBuffer(byte[] buffer, int length)
        {
            _buffer = buffer;
            _length = length;
        }

        public void Dispose()
        {
            if (_buffer != null)
            {
                ArrayPool<byte>.Shared.Return(_buffer);
                _buffer = null;
            }
        }
    }
}
