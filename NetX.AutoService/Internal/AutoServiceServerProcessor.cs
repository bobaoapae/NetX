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
        private readonly Func<IAutoServicePeerSession, IAutoServiceStrictAuthenticator> _authenticatorFactory;
        private readonly int _maxFrameBytes;
        private readonly int _maxConcurrentDispatches;
        private readonly int _maxPendingInbound;
        private readonly Func<IAutoServicePeerSession, CancellationToken, ValueTask> _onConnected;
        private readonly Func<IAutoServicePeerSession, DisconnectReason, ValueTask> _onDisconnected;
        private readonly ConcurrentDictionary<Guid, AutoServicePeerSession> _sessions = new();

        internal AutoServiceServerProcessor(
            AutoServiceRouter router,
            Func<IAutoServicePeerSession, IAutoServiceStrictAuthenticator> authenticatorFactory,
            int maxFrameBytes,
            int maxConcurrentDispatches,
            int maxPendingInbound,
            Func<IAutoServicePeerSession, CancellationToken, ValueTask> onConnected,
            Func<IAutoServicePeerSession, DisconnectReason, ValueTask> onDisconnected)
        {
            _router = router ?? throw new ArgumentNullException(nameof(router));
            _authenticatorFactory = authenticatorFactory ?? throw new ArgumentNullException(nameof(authenticatorFactory));
            _maxFrameBytes = maxFrameBytes;
            _maxConcurrentDispatches = maxConcurrentDispatches;
            _maxPendingInbound = maxPendingInbound;
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
            var peer = new AutoServicePeerSession(
                session,
                session.Id,
                new IPEndPoint(session.RemoteAddress, 0),
                _router,
                _authenticatorFactory,
                _maxFrameBytes,
                _maxConcurrentDispatches,
                _maxPendingInbound);

            _sessions[session.Id] = peer;

            if (_onConnected != null)
                await _onConnected(peer, cancellationToken).ConfigureAwait(false);
        }

        public ValueTask OnReceivedMessageAsync(INetXSession session, NetXMessage message, CancellationToken cancellationToken)
        {
            // NetXConnection.ReadPipeAsync awaits this method before it reads the next frame off the
            // wire. EnqueueInbound hands the message to this peer's bounded pipeline and returns
            // immediately, so the loop can still read reverse-call replies. The peer decodes FIFO,
            // keeps operation 0 as an inline barrier, and dispatches authenticated non-auth frames
            // concurrently within its configured slot limit.
            if (_sessions.TryGetValue(session.Id, out var peer))
                peer.EnqueueInbound(message, cancellationToken);
            else
                message.Dispose();

            return ValueTask.CompletedTask;
        }

        public async ValueTask OnSessionDisconnectAsync(Guid sessionId, DisconnectReason reason)
        {
            if (_sessions.TryRemove(sessionId, out var peer))
            {
                peer.CompleteInbound();

                if (_onDisconnected != null)
                    await _onDisconnected(peer, reason).ConfigureAwait(false);
            }
        }

        public void ProcessReceivedBuffer(INetXSession session, in ReadOnlyMemory<byte> buffer)
        {
        }

        public void ProcessSendBuffer(INetXSession session, in ReadOnlyMemory<byte> buffer)
        {
        }
    }
}
