using System;
using System.Threading;

namespace NetX.AutoService
{
    /// <summary>
    /// Ambient, per-dispatch identity of the <see cref="IAutoServicePeerSession"/> currently executing an
    /// inbound call. Set by <see cref="Internal.AutoServicePeerSession.HandleInboundAsync"/> around the
    /// dispatcher invocation -- covering requests, one-way calls and the op-0 auth handshake alike, on
    /// both the server side and the client's own reverse dispatch (both ride the same
    /// <c>HandleInboundAsync</c>). <see cref="Current"/> is null everywhere outside of that scope
    /// (background loops, timers, code that runs before/after a dispatch).
    /// </summary>
    public static class AutoServiceSessionContext
    {
        private static readonly AsyncLocal<IAutoServicePeerSession> _current = new();

        /// <summary>The peer session dispatching the call currently in flight on this async context, or null.</summary>
        public static IAutoServicePeerSession Current => _current.Value;

        /// <summary>Sets <see cref="Current"/> to <paramref name="session"/> until the returned scope is disposed, then restores the prior value.</summary>
        internal static Scope Begin(IAutoServicePeerSession session)
        {
            var previous = _current.Value;
            _current.Value = session;
            return new Scope(previous);
        }

        /// <summary>
        /// Restores the ambient <see cref="Current"/> to whatever it was before <see cref="Begin"/> was
        /// called. Disposal is idempotent -- a second (or later) <see cref="Dispose"/> call is a no-op,
        /// so a caller that disposes more than once (e.g. via both an explicit call and a <c>using</c>
        /// block) can never clobber a nested scope's restore with a stale <c>_previous</c> value.
        /// </summary>
        internal sealed class Scope : IDisposable
        {
            private readonly IAutoServicePeerSession _previous;
            private bool _disposed;

            internal Scope(IAutoServicePeerSession previous)
            {
                _previous = previous;
            }

            public void Dispose()
            {
                if (_disposed)
                    return;
                _disposed = true;
                _current.Value = _previous;
            }
        }
    }
}
