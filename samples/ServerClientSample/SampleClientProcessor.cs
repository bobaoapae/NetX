using System;
using System.Threading;
using System.Threading.Tasks;
using NetX;
using Serilog;

namespace ServerClientSample
{
    public class SampleClientProcessor : INetXClientProcessor
    {
        public ValueTask OnConnectedAsync(INetXConnection client, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask OnDisconnectedAsync(DisconnectReason reason)
        {
            Log.Information("Client disconnected. Reason: {reason}", reason);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnReceivedMessageAsync(INetXConnection client, NetXMessage message, CancellationToken cancellationToken)
        {
            // Ownership contract: this handler owns `message` and must dispose it once done
            // reading its Buffer. We finish synchronously here, so a `using` is enough — a handler
            // that offloads work to a background task must dispose there instead (see NetXMessage).
            using (message)
            {
                return ValueTask.CompletedTask;
            }
        }

        public void ProcessReceivedBuffer(INetXConnection client, in ReadOnlyMemory<byte> buffer)
        {
        }

        public void ProcessSendBuffer(INetXConnection client, in ReadOnlyMemory<byte> buffer)
        {
        }
    }
}
