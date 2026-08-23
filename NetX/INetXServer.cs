using System;
using System.Threading;
using System.Collections.Generic;

namespace NetX
{
    public interface INetXServer
    {
        void Listen(CancellationToken cancellationToken = default);

        /// <summary>
        /// Stops accepting new connections, closes the listening socket (freeing the bound port for
        /// reuse) and disconnects every currently connected session. Idempotent -- safe to call more
        /// than once, and safe to call even if <see cref="Listen"/> was cancelled via its own
        /// <see cref="CancellationToken"/> beforehand (that path also routes here).
        /// </summary>
        void Stop();

        bool TryGetSession(Guid sessionId, out INetXSession session);
        IEnumerable<INetXSession> GetAllSessions();
    }
}
