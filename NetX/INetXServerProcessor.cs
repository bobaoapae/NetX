using System;
using System.Threading;
using System.Threading.Tasks;

namespace NetX
{
    public interface INetXServerProcessor
    {
        ValueTask OnSessionConnectAsync(INetXSession session, CancellationToken cancellationToken);
        ValueTask OnReceivedMessageAsync(INetXSession session, NetXMessage message, CancellationToken cancellationToken);
        ValueTask OnSessionDisconnectAsync(Guid sessionId, DisconnectReason reason);

        void ProcessReceivedBuffer(INetXSession session, in ReadOnlyMemory<byte> buffer);
        void ProcessSendBuffer(INetXSession session, in ReadOnlyMemory<byte> buffer);
    }
}
