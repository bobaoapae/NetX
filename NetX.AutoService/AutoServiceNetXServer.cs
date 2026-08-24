using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NetX.AutoService.Internal;
using NetX.AutoServiceGenerator.Definitions;
using NetX.Options;

namespace NetX.AutoService
{
    /// <summary>Entry point for building a NetX-backed AutoService server.</summary>
    public static class AutoServiceNetXServer
    {
        public static AutoServiceNetXServerBuilder Create(ILoggerFactory loggerFactory = null, string serverName = null)
            => new(loggerFactory, serverName);
    }

    /// <summary>Fluent builder for an <see cref="AutoServiceNetXServerHost"/>.</summary>
    public sealed class AutoServiceNetXServerBuilder
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly string _serverName;
        private readonly AutoServiceRouter _router = new();

        private IPEndPoint _endPoint = new(IPAddress.Any, 0);
        private Func<IAutoServicePeerSession, IAutoServiceStrictAuthenticator> _authenticatorFactory = _ => AutoServiceNoAuthAuthenticator.Instance;
        private Func<IAutoServicePeerSession, CancellationToken, ValueTask> _onSessionConnected;
        private Func<IAutoServicePeerSession, DisconnectReason, ValueTask> _onSessionDisconnected;
        private int _duplexTimeoutMs = 5000;
        private int _maxFrameBytes = AutoServiceFrameCodec.DefaultMaxFrameBytes;
        private int _receiveBufferSize = 8192;
        private int _sendBufferSize = 8192;
        private bool _noDelay = true;
        private int _backlog = 100;
        private int _maxConcurrentDispatches = 64;
        private int _maxPendingInbound = 128;

        internal AutoServiceNetXServerBuilder(ILoggerFactory loggerFactory, string serverName)
        {
            _loggerFactory = loggerFactory;
            _serverName = serverName;
        }

        public AutoServiceNetXServerBuilder WithEndPoint(IPEndPoint endPoint)
        {
            _endPoint = endPoint ?? throw new ArgumentNullException(nameof(endPoint));
            return this;
        }

        public AutoServiceNetXServerBuilder WithEndPoint(string address, ushort port)
            => WithEndPoint(new IPEndPoint(IPAddress.Parse(address), port));

        /// <summary>
        /// Registers a service into the shared <see cref="AutoServiceRouter"/>. Repeatable: call once per
        /// distinct service contract hosted on this server. <typeparamref name="TService"/> is a
        /// compile-time tag only (there is no source generator wired into this project, so the caller
        /// supplies the already-built <paramref name="dispatcher"/> directly -- hand-written or generated
        /// elsewhere).
        /// </summary>
        public AutoServiceNetXServerBuilder WithService<TService>(AutoServiceContractDescriptor contract, IAutoServiceDispatcher dispatcher)
            where TService : class
        {
            _router.Add(contract, dispatcher);
            return this;
        }

        /// <summary>
        /// Configures the authenticator used for the operation-0 handshake. The factory is invoked once
        /// per accepted session, so the authenticator (and any per-connection state it captures) can vary
        /// across connections instead of being one shared/global instance.
        /// </summary>
        public AutoServiceNetXServerBuilder WithAuthenticator(Func<IAutoServiceStrictAuthenticator> authenticatorFactory)
        {
            if (authenticatorFactory == null)
                throw new ArgumentNullException(nameof(authenticatorFactory));
            _authenticatorFactory = _ => authenticatorFactory();
            return this;
        }

        public AutoServiceNetXServerBuilder WithAuthenticator(IAutoServiceStrictAuthenticator authenticator)
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
        public AutoServiceNetXServerBuilder WithAuthenticator(Func<IAutoServicePeerSession, IAutoServiceStrictAuthenticator> authenticatorFactory)
        {
            _authenticatorFactory = authenticatorFactory ?? throw new ArgumentNullException(nameof(authenticatorFactory));
            return this;
        }

        public AutoServiceNetXServerBuilder OnSessionConnected(Func<IAutoServicePeerSession, CancellationToken, ValueTask> callback)
        {
            _onSessionConnected = callback;
            return this;
        }

        public AutoServiceNetXServerBuilder OnSessionDisconnected(Func<IAutoServicePeerSession, DisconnectReason, ValueTask> callback)
        {
            _onSessionDisconnected = callback;
            return this;
        }

        public AutoServiceNetXServerBuilder WithLimits(int receiveBufferSize, int sendBufferSize)
        {
            _receiveBufferSize = receiveBufferSize;
            _sendBufferSize = sendBufferSize;
            return this;
        }

        public AutoServiceNetXServerBuilder WithDuplexTimeout(int timeoutMs)
        {
            _duplexTimeoutMs = timeoutMs;
            return this;
        }

        public AutoServiceNetXServerBuilder WithMaxFrameBytes(int maxFrameBytes)
        {
            if (maxFrameBytes <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxFrameBytes));
            _maxFrameBytes = maxFrameBytes;
            return this;
        }

        public AutoServiceNetXServerBuilder WithNoDelay(bool noDelay)
        {
            _noDelay = noDelay;
            return this;
        }

        public AutoServiceNetXServerBuilder WithBacklog(int backlog)
        {
            _backlog = backlog;
            return this;
        }

        /// <summary>
        /// Caps how many non-auth inbound dispatches (Request/OneWay) run concurrently per peer via a
        /// dispatch-slot semaphore -- bounds the reentrant-duplex fan-out that op-0's inline barrier
        /// doesn't cover. Non-auth completion order is not guaranteed. Nesting deeper than this cap
        /// resolves only when a transport timeout frees a slot or pending traffic overflows the queue
        /// and disconnects the peer. Default 64. Must be &gt; 0.
        /// </summary>
        public AutoServiceNetXServerBuilder WithMaxConcurrentDispatches(int maxConcurrentDispatches)
        {
            if (maxConcurrentDispatches <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxConcurrentDispatches));
            _maxConcurrentDispatches = maxConcurrentDispatches;
            return this;
        }

        /// <summary>
        /// Caps the per-peer inbound queue depth. A peer that overflows it is disconnected (never
        /// blocked -- see the peer session's inbound-queue doc for why blocking the read loop here
        /// would deadlock). Default 128. Must be &gt; 0.
        /// </summary>
        public AutoServiceNetXServerBuilder WithMaxPendingInbound(int maxPendingInbound)
        {
            if (maxPendingInbound <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxPendingInbound));
            _maxPendingInbound = maxPendingInbound;
            return this;
        }

        /// <summary>Builds and starts listening. Cancelling <paramref name="cancellationToken"/> shuts the server down.</summary>
        public Task<AutoServiceNetXServerHost> StartAsync(CancellationToken cancellationToken = default)
        {
            var processor = new AutoServiceServerProcessor(_router, _authenticatorFactory, _maxFrameBytes, _maxConcurrentDispatches, _maxPendingInbound, _onSessionConnected, _onSessionDisconnected);

            // Note: server-specific fluent methods (Backlog, UseProxy) live on INetXServerOptionsBuilder,
            // whose own methods (EndPoint, NoDelay, ...) return the narrower INetXConnectionOptionsBuilder<T>
            // interface -- so anything server-specific must be called *before* the first connection-level
            // fluent call, or it is no longer reachable on the chain.
            var netXServer = NetXServerBuilder.Create(_loggerFactory, _serverName)
                .Processor(processor)
                .Backlog(_backlog)
                .EndPoint(_endPoint)
                .NoDelay(_noDelay)
                .ReceiveBufferSize(_receiveBufferSize)
                .SendBufferSize(_sendBufferSize)
                .DuplexTimeout(_duplexTimeoutMs)
                .MaxFrameBytes(AutoServiceNetXFrameLimits.AddOverheadMargin(_maxFrameBytes))
                .Build();

            netXServer.Listen(cancellationToken);

            return Task.FromResult(new AutoServiceNetXServerHost(netXServer, processor));
        }
    }

    /// <summary>A running AutoService server. Disposing disconnects every currently connected session.</summary>
    public sealed class AutoServiceNetXServerHost : IDisposable
    {
        private readonly INetXServer _netXServer;
        private readonly AutoServiceServerProcessor _processor;

        internal AutoServiceNetXServerHost(INetXServer netXServer, AutoServiceServerProcessor processor)
        {
            _netXServer = netXServer;
            _processor = processor;
        }

        public INetXServer NetXServer => _netXServer;

        public bool TryGetSession(Guid sessionId, out IAutoServicePeerSession session) => _processor.TryGetSession(sessionId, out session);

        public IEnumerable<IAutoServicePeerSession> GetAllSessions() => _processor.Sessions;

        /// <summary>
        /// Stops the underlying listener (closing the socket so the port is free to rebind) and
        /// disconnects every currently connected session. Idempotent.
        /// </summary>
        public void Dispose()
        {
            // Stop() itself disconnects every INetXSession, but it doesn't know about the AutoService
            // peer wrapper this host tracks per session -- disconnect through the processor's own view
            // too so a caller inspecting host.GetAllSessions() sees every session torn down.
            _netXServer.Stop();

            foreach (var session in _processor.Sessions)
                session.Disconnect();
        }
    }
}
