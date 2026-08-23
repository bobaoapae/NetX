using System;
using System.Threading;
using System.Threading.Tasks;
using NetX.AutoServiceGenerator.Definitions;

namespace NetX.AutoService.Internal
{
    /// <summary>Bridges NetX client-side connection events to a single <see cref="AutoServicePeerSession"/>.</summary>
    internal sealed class AutoServiceClientProcessor : INetXClientProcessor
    {
        private readonly AutoServiceRouter _reverseRouter;
        private readonly Func<IAutoServicePeerSession, IAutoServiceStrictAuthenticator> _authenticatorFactory;
        private readonly int _maxFrameBytes;
        private readonly Func<IAutoServicePeerSession, CancellationToken, ValueTask> _onConnected;
        private readonly Func<DisconnectReason, ValueTask> _onDisconnected;
        private readonly TaskCompletionSource<AutoServicePeerSession> _peerReady =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private AutoServicePeerSession _peer;

        internal AutoServiceClientProcessor(
            AutoServiceRouter reverseRouter,
            Func<IAutoServicePeerSession, IAutoServiceStrictAuthenticator> authenticatorFactory,
            int maxFrameBytes,
            Func<IAutoServicePeerSession, CancellationToken, ValueTask> onConnected,
            Func<DisconnectReason, ValueTask> onDisconnected)
        {
            _reverseRouter = reverseRouter ?? throw new ArgumentNullException(nameof(reverseRouter));
            _authenticatorFactory = authenticatorFactory ?? (_ => AutoServiceNoAuthAuthenticator.Instance);
            _maxFrameBytes = maxFrameBytes;
            _onConnected = onConnected;
            _onDisconnected = onDisconnected;
        }

        /// <summary>Completes once <see cref="OnConnectedAsync"/> has created the peer session.</summary>
        internal Task<AutoServicePeerSession> PeerReady => _peerReady.Task;

        public async ValueTask OnConnectedAsync(INetXConnection client, CancellationToken cancellationToken)
        {
            _peer = new AutoServicePeerSession(client, Guid.NewGuid(), null, _reverseRouter, _authenticatorFactory, _maxFrameBytes);
            _peerReady.TrySetResult(_peer);

            if (_onConnected != null)
                await _onConnected(_peer, cancellationToken).ConfigureAwait(false);
        }

        public ValueTask OnReceivedMessageAsync(INetXConnection client, NetXMessage message, CancellationToken cancellationToken)
        {
            // NetXConnection.ReadPipeAsync awaits this method before it reads the next frame off the
            // wire. EnqueueInbound hands the message off to this peer's own serial inbound queue and
            // returns immediately, so this read loop can move straight on -- including to the reply
            // frame of a reverse call a dispatcher makes back into this same peer before it finishes
            // handling the message just queued here. See AutoServicePeerSession's queue field doc for
            // the full deadlock/ordering rationale. Symmetric with AutoServiceServerProcessor.
            var peer = _peer;
            if (peer != null)
                peer.EnqueueInbound(message, cancellationToken);
            else
                message.Dispose();

            return ValueTask.CompletedTask;
        }

        public async ValueTask OnDisconnectedAsync(DisconnectReason reason)
        {
            _peer?.CompleteInbound();
            _peerReady.TrySetException(new InvalidOperationException("The AutoService client disconnected before connecting."));

            if (_onDisconnected != null)
                await _onDisconnected(reason).ConfigureAwait(false);
        }

        public void ProcessReceivedBuffer(INetXConnection client, in ReadOnlyMemory<byte> buffer)
        {
        }

        public void ProcessSendBuffer(INetXConnection client, in ReadOnlyMemory<byte> buffer)
        {
        }
    }
}
