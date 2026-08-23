using System;
using System.Threading;
using CommunityToolkit.HighPerformance.Buffers;

namespace NetX
{
    /// <summary>
    /// A frame handed to the caller — either an inbound message dispatched to
    /// <see cref="INetXClientProcessor.OnReceivedMessageAsync"/>/<see cref="INetXServerProcessor.OnReceivedMessageAsync"/>,
    /// or the reply returned from <see cref="INetXConnection.RequestAsync(ReadOnlyMemory{byte}, System.TimeSpan, CancellationToken)"/>.
    ///
    /// Ownership contract: the underlying pooled buffer transfers to whoever holds this instance.
    /// Callers MUST call <see cref="Dispose"/> exactly once when done with <see cref="Buffer"/>
    /// (a `using`/`await using`-free `using var message = ...;` block is the simplest way).
    /// This type is a reference type specifically so it cannot be copied and double-disposed by
    /// value-semantics accidents; <see cref="Dispose"/> itself is idempotent (safe to call more than
    /// once), but <see cref="Buffer"/> throws <see cref="ObjectDisposedException"/> once disposed —
    /// the pooled array may already have been handed back out to a new caller by then.
    /// </summary>
    public sealed class NetXMessage : IDisposable
    {
        /// <summary>
        /// Correlation id of the frame this message was carried in.
        /// 0 means the frame is a push/one-way message (no reply expected).
        /// </summary>
        public ulong CorrelationId { get; }

        public bool IsPush => CorrelationId == 0;

        public ReadOnlyMemory<byte> Buffer
        {
            get
            {
                ObjectDisposedException.ThrowIf(_disposed != 0, this);
                return _memoryOwner.Memory;
            }
        }

        private readonly MemoryOwner<byte> _memoryOwner;
        private int _disposed;

        public NetXMessage(ulong correlationId, MemoryOwner<byte> memoryOwner)
        {
            CorrelationId = correlationId;
            _memoryOwner = memoryOwner;
        }

        public void Dispose()
        {
            // Idempotent: whichever caller disposes first returns the buffer to the pool;
            // later Dispose() calls (e.g. a defensive `using` at two layers) are no-ops
            // instead of a double-return-to-pool corruption.
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            _memoryOwner.Dispose();
        }
    }
}
