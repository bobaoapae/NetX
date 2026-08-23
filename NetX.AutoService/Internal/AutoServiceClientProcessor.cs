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
        private readonly IAutoServiceStrictAuthenticator _authenticator;
        private readonly int _maxFrameBytes;
        private readonly Func<IAutoServicePeerSession, CancellationToken, ValueTask> _onConnected;
        private readonly Func<DisconnectReason, ValueTask> _onDisconnected;
        private readonly TaskCompletionSource<AutoServicePeerSession> _peerReady =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private AutoServicePeerSession _peer;

        internal AutoServiceClientProcessor(
            AutoServiceRouter reverseRouter,
            IAutoServiceStrictAuthenticator authenticator,
            int maxFrameBytes,
            Func<IAutoServicePeerSession, CancellationToken, ValueTask> onConnected,
            Func<DisconnectReason, ValueTask> onDisconnected)
        {
            _reverseRouter = reverseRouter ?? throw new ArgumentNullException(nameof(reverseRouter));
            _authenticator = authenticator ?? AutoServiceNoAuthAuthenticator.Instance;
            _maxFrameBytes = maxFrameBytes;
            _onConnected = onConnected;
            _onDisconnected = onDisconnected;
        }

        /// <summary>Completes once <see cref="OnConnectedAsync"/> has created the peer session.</summary>
        internal Task<AutoServicePeerSession> PeerReady => _peerReady.Task;

        public async ValueTask OnConnectedAsync(INetXConnection client, CancellationToken cancellationToken)
        {
            _peer = new AutoServicePeerSession(client, Guid.NewGuid(), null, _reverseRouter, _authenticator, _maxFrameBytes);
            _peerReady.TrySetResult(_peer);

            if (_onConnected != null)
                await _onConnected(_peer, cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask OnReceivedMessageAsync(INetXConnection client, NetXMessage message, CancellationToken cancellationToken)
        {
            var peer = _peer;
            if (peer != null)
            {
                await peer.HandleInboundAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                message.Dispose();
            }
        }

        public async ValueTask OnDisconnectedAsync(DisconnectReason reason)
        {
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
