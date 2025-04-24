using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetX;
using Serilog;

namespace ServerClientSample
{
    public class SampleServerProcessor : INetXServerProcessor
    {
        public ValueTask OnSessionConnectAsync(INetXSession session, CancellationToken cancellationToken)
        {
            Console.WriteLine($"Session {session.Id} connected. Time = {session.ConnectionTime} Address = {session.RemoteAddress}");
            return ValueTask.CompletedTask;
        }

        public async ValueTask OnReceivedMessageAsync(INetXSession session, NetXMessage message, CancellationToken cancellationToken)
        {
            Log.Information("Received message from session {sessionId} with length {messageLength}", session.Id, message.Buffer.Length);
            Log.Information("Session received message");
            Log.Information("Session returned to function after delay");
            var sendBytes = "teste12345678910"u8.ToArray();
            Log.Information("Sending data length {dataLength} with data {data}", sendBytes.Length, Convert.ToHexString(sendBytes));
            await session.SendAsync(sendBytes);
        }

        public ValueTask OnSessionDisconnectAsync(Guid sessionId, DisconnectReason reason)
        {
            Log.Information("Session {sessionId} disconnected. Reason: {reason}", sessionId, reason);
            return ValueTask.CompletedTask;
        }

        public int GetReceiveMessageSize(INetXSession session, in ReadOnlyMemory<byte> buffer)
        {
            return buffer.Length;
        }

        public void ProcessReceivedBuffer(INetXSession session, in ReadOnlyMemory<byte> buffer)
        {
        }

        public void ProcessSendBuffer(INetXSession session, in ReadOnlyMemory<byte> buffer)
        {
        }
    }
}
