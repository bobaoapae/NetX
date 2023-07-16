using System;
using System.Threading;
using System.Threading.Tasks;
using NetX;
using Serilog;

namespace ServerClientSample
{
    public class SampleClientProcessor : INetXClientProcessor
    {
        public int GetReceiveMessageSize(INetXClientSession client, in ReadOnlyMemory<byte> buffer)
        {
            return default;
        }

        public ValueTask OnConnectedAsync(INetXClientSession client, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask OnDisconnectedAsync(DisconnectReason reason)
        {
            Log.Information("Client disconnected. Reason: {reason}", reason);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnReceivedMessageAsync(INetXClientSession client, NetXMessage message, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public void ProcessReceivedBuffer(INetXClientSession client, in ReadOnlyMemory<byte> buffer)
        {
        }

        public void ProcessSendBuffer(INetXClientSession client, in ReadOnlyMemory<byte> buffer)
        {
        }
    }
}
