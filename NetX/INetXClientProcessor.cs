using System;
using System.Threading;
using System.Threading.Tasks;

namespace NetX
{
    public interface INetXClientProcessor
    {
        ValueTask OnConnectedAsync(INetXConnection client, CancellationToken cancellationToken);
        ValueTask OnReceivedMessageAsync(INetXConnection client, NetXMessage message, CancellationToken cancellationToken);
        ValueTask OnDisconnectedAsync(DisconnectReason reason);

        void ProcessReceivedBuffer(INetXConnection client, in ReadOnlyMemory<byte> buffer);
        void ProcessSendBuffer(INetXConnection client, in ReadOnlyMemory<byte> buffer);
    }
}
