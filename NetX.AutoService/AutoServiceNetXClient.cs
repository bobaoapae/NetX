using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NetX.AutoService.Internal;
using NetX.AutoServiceGenerator.Definitions;
using NetX.Options;

namespace NetX.AutoService
{
    /// <summary>Entry point for building a NetX-backed AutoService client.</summary>
    public static class AutoServiceNetXClient
    {
        public static AutoServiceNetXClientBuilder Create(ILoggerFactory loggerFactory = null, string clientName = null)
            => new(loggerFactory, clientName);
    }

    /// <summary>Fluent builder that connects to an <see cref="AutoServiceNetXServerHost"/> (or any compatible peer).</summary>
    public sealed class AutoServiceNetXClientBuilder
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly string _clientName;
        private readonly AutoServiceRouter _reverseRouter = new();

        private IPEndPoint _endPoint = new(IPAddress.Loopback, 5000);
        private Func<IAutoServicePeerSession, IAutoServiceStrictAuthenticator> _authenticatorFactory = _ => AutoServiceNoAuthAuthenticator.Instance;
        private Func<IAutoServicePeerSession, CancellationToken, ValueTask> _onConnected;
        private Func<DisconnectReason, ValueTask> _onDisconnected;
        private int _duplexTimeoutMs = 5000;
        private int _maxFrameBytes = AutoServiceFrameCodec.DefaultMaxFrameBytes;
        private int _receiveBufferSize = 8192;
        private int _sendBufferSize = 8192;
        private bool _noDelay = true;

        internal AutoServiceNetXClientBuilder(ILoggerFactory loggerFactory, string clientName)
        {
            _loggerFactory = loggerFactory;
            _clientName = clientName;
        }

        public AutoServiceNetXClientBuilder WithEndPoint(IPEndPoint endPoint)
        {
            _endPoint = endPoint ?? throw new ArgumentNullException(nameof(endPoint));
            return this;
        }

        public AutoServiceNetXClientBuilder WithEndPoint(string address, ushort port)
            => WithEndPoint(new IPEndPoint(IPAddress.Parse(address), port));

        /// <summary>
        /// Registers a *reverse* service: one the server can call back into on this client connection.
        /// Repeatable, same semantics as <see cref="AutoServiceNetXServerBuilder.WithService{TService}"/>.
        /// </summary>
        public AutoServiceNetXClientBuilder WithService<TService>(AutoServiceContractDescriptor contract, IAutoServiceDispatcher dispatcher)
            where TService : class
        {
            _reverseRouter.Add(contract, dispatcher);
            return this;
        }

        /// <summary>
        /// Configures the authenticator that gates *inbound* (server-initiated, reverse) calls on this
        /// client connection's own operation-0 handshake. Not required for the client's own outbound
        /// <see cref="IAutoServicePeerSession.AuthenticateAsync"/> call against the server.
        /// </summary>
        public AutoServiceNetXClientBuilder WithAuthenticator(Func<IAutoServiceStrictAuthenticator> authenticatorFactory)
        {
            if (authenticatorFactory == null)
                throw new ArgumentNullException(nameof(authenticatorFactory));
            _authenticatorFactory = _ => authenticatorFactory();
            return this;
        }

        public AutoServiceNetXClientBuilder WithAuthenticator(IAutoServiceStrictAuthenticator authenticator)
        {
            if (authenticator == null)
                throw new ArgumentNullException(nameof(authenticator));
            _authenticatorFactory = _ => authenticator;
            return this;
        }

        /// <summary>
        /// Session-aware overload: the factory receives the <see cref="IAutoServicePeerSession"/> being
        /// constructed so the authenticator can capture its identity. The session is not dispatch-ready
        /// while the factory runs; only connection identity and endpoint state should be inspected there.
        /// </summary>
        public AutoServiceNetXClientBuilder WithAuthenticator(Func<IAutoServicePeerSession, IAutoServiceStrictAuthenticator> authenticatorFactory)
        {
            _authenticatorFactory = authenticatorFactory ?? throw new ArgumentNullException(nameof(authenticatorFactory));
            return this;
        }

        public AutoServiceNetXClientBuilder OnConnected(Func<IAutoServicePeerSession, CancellationToken, ValueTask> callback)
        {
            _onConnected = callback;
            return this;
        }

        public AutoServiceNetXClientBuilder OnDisconnected(Func<DisconnectReason, ValueTask> callback)
        {
            _onDisconnected = callback;
            return this;
        }

        public AutoServiceNetXClientBuilder WithLimits(int receiveBufferSize, int sendBufferSize)
        {
            _receiveBufferSize = receiveBufferSize;
            _sendBufferSize = sendBufferSize;
            return this;
        }

        public AutoServiceNetXClientBuilder WithDuplexTimeout(int timeoutMs)
        {
            _duplexTimeoutMs = timeoutMs;
            return this;
        }

        public AutoServiceNetXClientBuilder WithMaxFrameBytes(int maxFrameBytes)
        {
            if (maxFrameBytes <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxFrameBytes));
            _maxFrameBytes = maxFrameBytes;
            return this;
        }

        public AutoServiceNetXClientBuilder WithNoDelay(bool noDelay)
        {
            _noDelay = noDelay;
            return this;
        }

        /// <summary>Connects and waits for the local <see cref="IAutoServicePeerSession"/> to be ready.</summary>
        public async Task<AutoServiceNetXClientConnection> ConnectAsync(CancellationToken cancellationToken = default)
        {
            var processor = new AutoServiceClientProcessor(_reverseRouter, _authenticatorFactory, _maxFrameBytes, _onConnected, _onDisconnected);

            var netXClient = NetXClientBuilder.Create(_loggerFactory, _clientName)
                .Processor(processor)
                .EndPoint(_endPoint)
                .NoDelay(_noDelay)
                .ReceiveBufferSize(_receiveBufferSize)
                .SendBufferSize(_sendBufferSize)
                .DuplexTimeout(_duplexTimeoutMs)
                .MaxFrameBytes(AutoServiceNetXFrameLimits.AddOverheadMargin(_maxFrameBytes))
                .Build();

            await netXClient.ConnectAsync(cancellationToken).ConfigureAwait(false);

            // NetXClient.ConnectAsync fires OnConnectedAsync fire-and-forget and returns as soon as the
            // socket connect completes -- wait for the processor to have actually built the peer session
            // so callers observe a fully-initialized AutoServiceNetXClientConnection.
            var peer = await processor.PeerReady.WaitAsync(cancellationToken).ConfigureAwait(false);

            return new AutoServiceNetXClientConnection(netXClient, peer);
        }
    }

    /// <summary>A connected AutoService client. Disposing disconnects the underlying NetX connection.</summary>
    public sealed class AutoServiceNetXClientConnection : IDisposable
    {
        private readonly INetXClient _netXClient;

        internal AutoServiceNetXClientConnection(INetXClient netXClient, IAutoServicePeerSession peer)
        {
            _netXClient = netXClient;
            Peer = peer;
        }

        public INetXClient NetXClient => _netXClient;

        /// <summary>The peer session for this connection -- send requests/one-way calls, inspect auth state, etc.</summary>
        public IAutoServicePeerSession Peer { get; }

        /// <summary>Convenience passthrough for the operation-0 handshake against the server.</summary>
        public Task<AutoServiceResponse> AuthenticateAsync(ReadOnlyMemory<byte> credential, CancellationToken cancellationToken = default)
            => Peer.AuthenticateAsync(credential, cancellationToken);

        public void Dispose() => _netXClient.Disconnect();
    }
}
