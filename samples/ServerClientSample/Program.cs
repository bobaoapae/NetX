using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NetX;
using NetX.Options;
using Serilog;

namespace ServerClientSample
{
    public class Program
    {
        private static INetXServer _server;
        private static INetXClient _client;

        public static async Task Main(string[] args)
        {
            var serverTokenSrc = new CancellationTokenSource();
            var clientTokenSrc = new CancellationTokenSource();

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Is(Serilog.Events.LogEventLevel.Verbose)
                .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:l}{NewLine}{Exception}")
                .CreateLogger();

            var loggerFactory = new LoggerFactory()
                .AddSerilog(Log.Logger);

            _server = NetXServerBuilder.Create(loggerFactory, "SampleServer")
                .Processor<SampleServerProcessor>()
                .EndPoint("0.0.0.0", 38101)
                .Duplex(true)
                .CopyBuffer(true)
                .NoDelay(true)
                .SocketTimeout(1000)
                .ReceiveBufferSize(20000020)
                .SendBufferSize(20000020)
                .Build();

            _server.Listen(serverTokenSrc.Token);

            _client = NetXClientBuilder.Create(loggerFactory, "SampleClient")
                .Processor<SampleClientProcessor>()
                .EndPoint("127.0.0.1", 38101)
                .Duplex(true)
                .CopyBuffer(true)
                .NoDelay(true)
                .SocketTimeout(1000)
                .ReceiveBufferSize(20000020)
                .SendBufferSize(20000020)
                .Build();

            await _client.ConnectAsync(clientTokenSrc.Token);

            await _client.SendAsync(new byte[] { 0 });
            await Task.Delay(500);
            _client.Disconnect();

            Console.ReadLine();
            await Task.Delay(3000);
        }
    }
}