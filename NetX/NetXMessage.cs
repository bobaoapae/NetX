using System;

namespace NetX
{
    public readonly struct NetXMessage
    {
        public Guid Id { get; }
        public ReadOnlyMemory<byte> Buffer { get; }

        public NetXMessage(Guid id, ReadOnlyMemory<byte> buffer)
        {
            Id = id;
            Buffer = buffer;
        }
    }
}
