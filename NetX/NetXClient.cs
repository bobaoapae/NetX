using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NetX.Options;

namespace NetX
{
    public class NetXClient : NetXConnection, INetXClient
    {
        private readonly string _clientName;

        // Tracked so ProcessClientConnection's disconnect dispatch can wait for this to fully finish
        // first -- see remarks there.
        private Task _connectDispatch = Task.CompletedTask;

        internal NetXClient(NetXClientOptions options, ILoggerFactory loggerFactory = null, string clientName = null)
            : base(CreateSocket(options.EndPoint), options, clientName, loggerFactory?.CreateLogger<NetXClient>(), isClientRole: true, options.ReuseSocket)
        {
            _clientName = clientName ?? nameof(NetXClient);
        }

        // Dual-mode when the target is IPv6 so both AAAA/IPv6-mapped and plain IPv6 endpoints work;
        // plain IPv4 targets keep using an IPv4 socket (works against dual-mode listeners too).
        private static Socket CreateSocket(System.Net.IPEndPoint endPoint)
        {
            var family = endPoint.AddressFamily == AddressFamily.InterNetworkV6
                ? AddressFamily.InterNetworkV6
                : AddressFamily.InterNetwork;

            var socket = new Socket(family, SocketType.Stream, ProtocolType.Tcp);
            if (family == AddressFamily.InterNetworkV6)
                socket.DualMode = true;

            return socket;
        }

        public async Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            await _socket.ConnectAsync(_options.EndPoint, cancellationToken);

            // Not awaited here -- see NetXServer.ProcessSessionConnection for why: a connect callback
            // that itself calls back into this connection (e.g. a duplex request) needs the pump loop
            // below already draining the send pipe, so it cannot be awaited before that loop starts.
            // The disconnect dispatch below still always waits for this to finish first.
            _connectDispatch = DispatchOnClientConnect(cancellationToken);

            _logger?.LogInformation("{name}: TCP Client connected to {address}:{port}", _clientName, _options.EndPoint.Address, _options.EndPoint.Port);

            _ = Task.Factory.StartNew(() => ProcessClientConnection(cancellationToken), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private async Task ProcessClientConnection(CancellationToken cancellationToken)
        {
            try
            {
                await ProcessConnection(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger?.LogCritical(ex, "{name}: An exception was throwed on process pipe", _clientName);
            }
            finally
            {
                _logger?.LogInformation("{name}: TCP Client disconnected. Reason({reason})",
                    _clientName, DisconnectReason);

                // Wait for the connect callback to fully finish (success or failure -- it never throws
                // past DispatchOnClientConnect's own try/catch) before ever starting the disconnect
                // callback, so a caller always observes them in connect-then-disconnect order.
                await _connectDispatch;
                await ((NetXClientOptions)_options).Processor.OnDisconnectedAsync(DisconnectReason);
            }
        }

        private async Task DispatchOnClientConnect(CancellationToken cancellationToken)
        {
            try
            {
                await ((NetXClientOptions)_options).Processor.OnConnectedAsync(this, cancellationToken);
            }
            catch (Exception e)
            {
                _logger?.LogError(e, "{svrName}: Fail on dispatch OnConnectedAsync to client session", _clientName);
            }
        }

        protected override ValueTask OnReceivedMessageAsync(NetXMessage message, CancellationToken cancellationToken)
            => ((NetXClientOptions)_options).Processor.OnReceivedMessageAsync(this, message, cancellationToken);

        protected override void ProcessReceivedBuffer(in ReadOnlyMemory<byte> buffer)
            => ((NetXClientOptions)_options).Processor.ProcessReceivedBuffer(this, in buffer);

        protected override void ProcessSendBuffer(in ReadOnlyMemory<byte> buffer)
            => ((NetXClientOptions)_options).Processor.ProcessSendBuffer(this, in buffer);
    }
}
