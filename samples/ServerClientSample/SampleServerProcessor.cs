using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetX;

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
            var messageId = message.Id;
            var responseMess = message.Buffer.ToArray();
            var textMessage = Encoding.UTF8.GetString(message.Buffer.Span);
            var token = cancellationToken;
            await session.ReplyAsync(messageId, responseMess, token);
        }

        public ValueTask OnSessionDisconnectAsync(Guid sessionId)
        {
            Console.WriteLine($"Session {sessionId} disconnected");
            return ValueTask.CompletedTask;
        }

        public int GetReceiveMessageSize(INetXSession session, in ReadOnlyMemory<byte> buffer)
        {
            return default;
        }

        public void ProcessReceivedBuffer(INetXSession session, in ReadOnlyMemory<byte> buffer)
        {
        }

        public void ProcessSendBuffer(INetXSession session, in ReadOnlyMemory<byte> buffer)
        {
        }
    }
}
