using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetX;
using Serilog;

namespace ServerClientSample
{
    public class SampleServerProcessor : INetXServerProcessor
    {
        public async Task OnSessionConnectAsync(INetXSession session, CancellationToken cancellationToken)
        {
            Console.WriteLine($"Session {session.Id} connected. Time = {session.ConnectionTime} Address = {session.RemoteAddress}");
        }

        public Task OnSessionDisconnectAsync(Guid sessionId)
        {
            Console.WriteLine($"Session {sessionId} disconnected");

            return Task.CompletedTask;
        }

        public Task OnReceivedMessageAsync(INetXSession session, NetXMessage message, CancellationToken cancellationToken)
        {
            _ = Task.Run(async () =>
            {
                var textMessage = Encoding.UTF8.GetString(message.Buffer);
                Log.Debug("Received message from {SessionId} with {Message}", session.Id, textMessage);
                var random = new Random();
                var data = Enumerable.Range(0, 20_000_000).Select(x => (byte)random.Next(9)).ToArray();
                Log.Debug("Sending big message to {SessionId} with lenght {Lenght}", session.Id, data.Length);
                await session.ReplyAsync(message.Id, data, cancellationToken);
            }, cancellationToken);
            
            return Task.CompletedTask;
        }

        public int GetReceiveMessageSize(INetXSession session, in ArraySegment<byte> buffer)
        {
            return 4;
        }

        public void ProcessReceivedBuffer(INetXSession session, in ArraySegment<byte> buffer)
        {
        }

        public void ProcessSendBuffer(INetXSession session, in ArraySegment<byte> buffer)
        {
        }
    }
}
