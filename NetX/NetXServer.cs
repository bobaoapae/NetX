using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
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

        internal NetXServer(NetXServerOptions options, ILoggerFactory loggerFactory = null, string serverName = null)
        {
            _logger = loggerFactory?.CreateLogger<NetXServer>();
            _serverName = serverName ?? nameof(NetXServer);

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = options.NoDelay,
                LingerState = new LingerOption(true, 5)
            };

            _socket.Bind(options.EndPoint);
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

            _logger?.LogInformation("{svrName}: Tcp server listening on {ip}:{port}", _serverName, _options.EndPoint.Address, _options.EndPoint.Port);

            _ = Task.Factory.StartNew(
                () => StartAcceptAsync(cancellationToken), 
                default,
                TaskCreationOptions.LongRunning, 
                TaskScheduler.Default);
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
                            () => ProcessSessionConnection(sessionSocket, listenCancellationToken), 
                            default, 
                            TaskCreationOptions.LongRunning, 
                            TaskScheduler.Default);
                    }
                    catch (TaskCanceledException) { }
                    catch (OperationCanceledException) { }
                    catch (SocketException e)
                    {
                        _logger?.LogError(e, "{svrName}: An exception was throw on accept new connections", _serverName);
                    }
                }

                _logger?.LogInformation("Shutdown {svrName} TCP server", _serverName);
            }
            catch (Exception ex)
            {
                _logger?.LogCritical(ex, "{svrName}: An exception was throw on listen pipe", _serverName);
            }
        }

        private async Task ProcessSessionConnection(Socket sessionSocket, CancellationToken cancellationToken)
        {
            try
            {
                var remoteAddress = ((IPEndPoint)sessionSocket.RemoteEndPoint).Address.MapToIPv4();
                if (_options.UseProxy)
                {
                    await using var stream = new NetworkStream(sessionSocket);
                    var proxyprotocol = new ProxyProtocol(stream, sessionSocket.RemoteEndPoint as IPEndPoint);
                    var realRemoteEndpoint = await proxyprotocol.GetRemoteEndpoint();
                    remoteAddress = realRemoteEndpoint.Address.MapToIPv4();
                }

                var session = new NetXSession(sessionSocket, remoteAddress, _options, _serverName, _logger);
                if (!_sessions.TryAdd(session.Id, session))
                {
                    session.Disconnect();
                    return;
                }

                _ = DispatchOnSessionConnect(session, cancellationToken);

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
                        await _options.Processor.OnSessionDisconnectAsync(session.Id, session.DisconnectReason);
                    }
                }
            }
            catch { }
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