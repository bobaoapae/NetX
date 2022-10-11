using System;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Sockets;
using NetX.Options;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;

namespace NetX
{
    public class NetXClient : NetXConnection, INetXClient
    {
        private readonly string _clientName;

        internal NetXClient(NetXClientOptions options, ILoggerFactory loggerFactory = null, string clientName = null)
            : base(new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp), options, clientName, loggerFactory?.CreateLogger<NetXClient>(), true)
        {
            _clientName = clientName ?? nameof(NetXClient);
        }

        public async Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            await _socket.ConnectAsync(_options.EndPoint, cancellationToken);

            _ = DispatchOnClientConnect(cancellationToken);

            _logger?.LogInformation("{name}: Tcp client connected to {address}:{port}", _clientName, _options.EndPoint.Address, _options.EndPoint.Port);
            
            _ = Task.Factory.StartNew(() =>
            {
                AsyncContext.Run(() => ProcessClientConnection(cancellationToken));
            }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
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
                await ((NetXClientOptions) _options).Processor.OnDisconnectedAsync();
            }
        }

        private async Task DispatchOnClientConnect(CancellationToken cancellationToken)
        {
            try
            {
                await ((NetXClientOptions) _options).Processor.OnConnectedAsync(this, cancellationToken);
            }
            catch (Exception e)
            {
                _logger?.LogError(e, "{svrName}: Fail on dispatch OnConnectedAsync to client session", _clientName);
            }
        }

        protected override Task OnReceivedMessageAsync(NetXMessage message, CancellationToken cancellationToken)
            => ((NetXClientOptions) _options).Processor.OnReceivedMessageAsync(this, message, cancellationToken);

        protected override int GetReceiveMessageSize(in ArraySegment<byte> buffer)
            => ((NetXClientOptions) _options).Processor.GetReceiveMessageSize(this, in buffer);

        protected override void ProcessReceivedBuffer(in ArraySegment<byte> buffer)
            => ((NetXClientOptions) _options).Processor.ProcessReceivedBuffer(this, in buffer);

        protected override void ProcessSendBuffer(in ArraySegment<byte> buffer)
            => ((NetXClientOptions) _options).Processor.ProcessSendBuffer(this, in buffer);
    }
}