using System;
using System.Buffers;
using CommunityToolkit.HighPerformance.Buffers;

namespace NetX
{
    public readonly struct NetXMessage : IDisposable
    {
        public Guid Id { get; }
        public ReadOnlyMemory<byte> Buffer => _memoryOwner.Memory;

        private readonly MemoryOwner<byte> _memoryOwner;

        public NetXMessage(Guid id, MemoryOwner<byte> memoryOwner)
        {
            Id = id;
            _memoryOwner = memoryOwner;
        }

        public void Dispose()
        {
            _memoryOwner.Dispose();
        }
    }
}
