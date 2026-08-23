using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Versioning;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NetX.Options;

namespace NetX
{
    public class NetXServer : INetXServer
    {
        private readonly ILogger _logger;
        private readonly string _serverName;
        private readonly Socket _socket;
        private readonly NetXServerOptions _options;
        private readonly ConcurrentDictionary<Guid, INetXSession> _sessions;

        // Independent of whatever CancellationToken a caller passes to Listen(): Stop() must fully
        // shut the listener down (close the socket so the port is free to rebind) even when Listen()
        // was called with the default token, or when Stop() is invoked directly instead of through
        // cancellation. Cancelling the caller-supplied token still routes here via the registration
        // in Listen(), so both shutdown paths converge on the same, single-fire logic.
        private readonly CancellationTokenSource _shutdownCts = new();
        private int _stopped;

        internal NetXServer(NetXServerOptions options, ILoggerFactory loggerFactory = null, string serverName = null)
        {
            _logger = loggerFactory?.CreateLogger<NetXServer>();
            _serverName = serverName ?? nameof(NetXServer);

            // IPv6 dual-mode listening socket: accepts both native IPv6 clients and IPv4 clients
            // (delivered as IPv4-mapped IPv6 addresses) on a single socket/port.
            _socket = new Socket(AddressFamily.InterNetworkV6, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = options.NoDelay,
                LingerState = new LingerOption(true, 5),
                DualMode = true
            };

            _socket.Bind(ToDualModeBindEndPoint(options.EndPoint));
            _socket.ReceiveTimeout = options.SocketTimeout;
            _socket.SendTimeout = options.SocketTimeout;
            _socket.ReceiveBufferSize = options.RecvBufferSize;
            _socket.SendBufferSize = options.SendBufferSize;

            _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer, options.RecvBufferSize);
            _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendBuffer, options.SendBufferSize);
            _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, options.SocketTimeout);
            _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout, options.SocketTimeout);

            _options = options;
            _sessions = new ConcurrentDictionary<Guid, INetXSession>();
        }

        private static IPEndPoint ToDualModeBindEndPoint(IPEndPoint endPoint)
        {
            if (endPoint.AddressFamily == AddressFamily.InterNetworkV6)
                return endPoint;

            return endPoint.Address.Equals(IPAddress.Any)
                ? new IPEndPoint(IPAddress.IPv6Any, endPoint.Port)
                : new IPEndPoint(endPoint.Address.MapToIPv6(), endPoint.Port);
        }

        private static IPAddress NormalizeRemoteAddress(IPAddress address)
            => address.IsIPv4MappedToIPv6 ? address.MapToIPv4() : address;

        public bool TryGetSession(Guid sessionId, out INetXSession session)
        {
            return _sessions.TryGetValue(sessionId, out session);
        }

        public IEnumerable<INetXSession> GetAllSessions()
        {
            return _sessions.Values;
        }

        public void Listen(CancellationToken cancellationToken = default)
        {
            _socket.Listen(_options.Backlog);

            _logger?.LogInformation("{svrName}: TCP Server listening on {ip}:{port}", _serverName, _options.EndPoint.Address, _options.EndPoint.Port);

            // Cancelling the caller-supplied token must fully stop the server (close the listening
            // socket, disconnect sessions), not merely stop the accept loop -- otherwise the port
            // stays bound after the caller believes the server is down. Routing through Stop() keeps
            // this the single place that does that, whether triggered by this token or by an explicit
            // Stop() call.
            cancellationToken.Register(Stop);

            var acceptToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token).Token;

            _ = Task.Factory.StartNew(
                () => StartAcceptAsync(acceptToken),
                default,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        /// <summary>
        /// Stops accepting new connections, closes the listening socket and disconnects every
        /// currently connected session. Safe to call more than once (only the first call has any
        /// effect) and safe to call whether or not <see cref="Listen"/>'s own token was ever cancelled.
        /// </summary>
        public void Stop()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
                return;

            _shutdownCts.Cancel();

            try
            {
                _socket.Close();
            }
            catch (Exception ex)
            {
                _logger?.LogDebug(ex, "{svrName}: Exception while closing the listening socket during Stop", _serverName);
            }

            foreach (var session in _sessions.Values)
                session.Disconnect();
        }

        private async Task StartAcceptAsync(CancellationToken listenCancellationToken)
        {
            try
            {
                while (!listenCancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var sessionSocket = await _socket.AcceptAsync(listenCancellationToken);

                        _ = Task.Factory.StartNew(
                            () => ProcessSessionConnection(sessionSocket, (IPEndPoint)sessionSocket.RemoteEndPoint, listenCancellationToken),
                            default,
                            TaskCreationOptions.LongRunning,
                            TaskScheduler.Default);
                    }
                    catch (TaskCanceledException)
                    {
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (ObjectDisposedException)
                    {
                        // Stop() closed the listening socket while an accept was pending -- expected on
                        // shutdown, not an error. _shutdownCts is already cancelled by the time Stop()
                        // reaches here, so the loop condition below ends the loop on the next check.
                    }
                    catch (SocketException e)
                    {
                        _logger?.LogError(e, "{svrName}: An exception was throw on accept new connections", _serverName);
                    }
                }

                _logger?.LogInformation("{svrName}: TCP server shutdown", _serverName);
            }
            catch (Exception ex)
            {
                _logger?.LogCritical(ex, "{svrName}: An exception was throw on listen pipe", _serverName);
            }
        }

        /**
         * The unique purpose of expose this method it's to allow the library to be used in a proxy environment using Socket Duplicate And Close.
         */
        [SupportedOSPlatform("windows")]
        public async Task UnsafeProcessSessionConnection(Socket sessionSocket, IPEndPoint ipEndPoint, CancellationToken cancellationToken)
        {
            await ProcessSessionConnection(sessionSocket, ipEndPoint, cancellationToken);
        }

        private async Task ProcessSessionConnection(Socket sessionSocket, IPEndPoint ipEndPoint, CancellationToken cancellationToken)
        {
            try
            {
                var remoteAddress = NormalizeRemoteAddress(ipEndPoint.Address);
                if (_options.UseProxy)
                {
                    await using var stream = new NetworkStream(sessionSocket);
                    var proxyprotocol = new ProxyProtocol(stream, sessionSocket.RemoteEndPoint as IPEndPoint);
                    var realRemoteEndpoint = await proxyprotocol.GetRemoteEndpoint();
                    remoteAddress = NormalizeRemoteAddress(realRemoteEndpoint.Address);
                }

                var session = new NetXSession(sessionSocket, remoteAddress, _options, _serverName, _logger);
                if (!_sessions.TryAdd(session.Id, session))
                {
                    session.Disconnect();
                    return;
                }

                // Still started without waiting for it, so a connect callback that itself calls back
                // into the connection (e.g. a duplex request that only completes once the pump loop
                // below is actually draining the send pipe) does not deadlock against its own connect.
                // What must never happen is the disconnect callback overtaking a still-in-flight connect
                // callback for the same session -- that inversion is fixed below, not by serializing
                // this start.
                var connectDispatch = DispatchOnSessionConnect(session, cancellationToken);

                try
                {
                    await session.ProcessConnection(cancellationToken);
                }
                catch
                {
                    session.Disconnect();
                }
                finally
                {
                    if (_sessions.TryRemove(session.Id, out var netXSession))
                    {
                        // Wait for the connect callback to fully finish (success or failure -- it never
                        // throws past DispatchOnSessionConnect's own try/catch) before ever starting the
                        // disconnect callback for this session, so a caller always observes them in
                        // connect-then-disconnect order, never inverted or interleaved.
                        await connectDispatch;
                        await _options.Processor.OnSessionDisconnectAsync(session.Id, session.DisconnectReason);
                    }
                }
            }
            catch
            {
            }
        }

        private async Task DispatchOnSessionConnect(INetXSession session, CancellationToken cancellationToken)
        {
            try
            {
                await _options.Processor.OnSessionConnectAsync(session, cancellationToken);
            }
            catch (Exception e)
            {
                _logger?.LogError(e, "{svrName}: Fail on dispatch OnSessionConnectAsync to session {sessId}", _serverName, session.Id);
            }
        }
    }
}