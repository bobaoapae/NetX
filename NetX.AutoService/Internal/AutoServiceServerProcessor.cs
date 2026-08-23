using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using NetX.AutoServiceGenerator.Definitions;

namespace NetX.AutoService.Internal
{
    /// <summary>Bridges NetX server-side session events to one <see cref="AutoServicePeerSession"/> per session.</summary>
    internal sealed class AutoServiceServerProcessor : INetXServerProcessor
    {
        private readonly AutoServiceRouter _router;
        private readonly Func<IAutoServiceStrictAuthenticator> _authenticatorFactory;
        private readonly int _maxFrameBytes;
        private readonly Func<IAutoServicePeerSession, CancellationToken, ValueTask> _onConnected;
        private readonly Func<IAutoServicePeerSession, DisconnectReason, ValueTask> _onDisconnected;
        private readonly ConcurrentDictionary<Guid, AutoServicePeerSession> _sessions = new();

        internal AutoServiceServerProcessor(
            AutoServiceRouter router,
            Func<IAutoServiceStrictAuthenticator> authenticatorFactory,
            int maxFrameBytes,
            Func<IAutoServicePeerSession, CancellationToken, ValueTask> onConnected,
            Func<IAutoServicePeerSession, DisconnectReason, ValueTask> onDisconnected)
        {
            _router = router ?? throw new ArgumentNullException(nameof(router));
            _authenticatorFactory = authenticatorFactory ?? throw new ArgumentNullException(nameof(authenticatorFactory));
            _maxFrameBytes = maxFrameBytes;
            _onConnected = onConnected;
            _onDisconnected = onDisconnected;
        }

        internal IEnumerable<IAutoServicePeerSession> Sessions => _sessions.Values;

        internal bool TryGetSession(Guid sessionId, out IAutoServicePeerSession session)
        {
            if (_sessions.TryGetValue(sessionId, out var peer))
            {
                session = peer;
                return true;
            }

            session = null;
            return false;
        }

        public async ValueTask OnSessionConnectAsync(INetXSession session, CancellationToken cancellationToken)
        {
            var authenticator = _authenticatorFactory() ?? AutoServiceNoAuthAuthenticator.Instance;
            var peer = new AutoServicePeerSession(
                session,
                session.Id,
                new IPEndPoint(session.RemoteAddress, 0),
                _router,
                authenticator,
                _maxFrameBytes);

            _sessions[session.Id] = peer;

            if (_onConnected != null)
                await _onConnected(peer, cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask OnReceivedMessageAsync(INetXSession session, NetXMessage message, CancellationToken cancellationToken)
        {
            if (_sessions.TryGetValue(session.Id, out var peer))
            {
                await peer.HandleInboundAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                message.Dispose();
            }
        }

        public async ValueTask OnSessionDisconnectAsync(Guid sessionId, DisconnectReason reason)
        {
            if (_sessions.TryRemove(sessionId, out var peer) && _onDisconnected != null)
                await _onDisconnected(peer, reason).ConfigureAwait(false);
        }

        public void ProcessReceivedBuffer(INetXSession session, in ReadOnlyMemory<byte> buffer)
        {
        }

        public void ProcessSendBuffer(INetXSession session, in ReadOnlyMemory<byte> buffer)
        {
        }
    }
}
