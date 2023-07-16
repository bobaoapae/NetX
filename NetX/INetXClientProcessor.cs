using System;
using System.Threading;
using System.Threading.Tasks;

namespace NetX
{
    public interface INetXClientProcessor
    {
        ValueTask OnConnectedAsync(INetXClientSession client, CancellationToken cancellationToken);
        ValueTask OnReceivedMessageAsync(INetXClientSession client, NetXMessage message, CancellationToken cancellationToken);
        ValueTask OnDisconnectedAsync(DisconnectReason reason);

        int GetReceiveMessageSize(INetXClientSession client, in ReadOnlyMemory<byte> buffer);
        void ProcessReceivedBuffer(INetXClientSession client, in ReadOnlyMemory<byte> buffer);
        void ProcessSendBuffer(INetXClientSession client, in ReadOnlyMemory<byte> buffer);
    }
}
