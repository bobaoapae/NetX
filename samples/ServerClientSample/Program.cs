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
                .EndPoint("0.0.0.0", 48101)
                .Duplex(false)
                .CopyBuffer(true)
                .NoDelay(true)
                .SocketTimeout(1000)
                .ReceiveBufferSize(20000020)
                .SendBufferSize(20000020)
                .Build();

            _server.Listen(serverTokenSrc.Token);

            /*_client = NetXClientBuilder.Create(loggerFactory, "SampleClient")
                .Processor<SampleClientProcessor>()
                .EndPoint("127.0.0.1", 38101)
                .Duplex(false)
                .CopyBuffer(true)
                .NoDelay(true)
                .SocketTimeout(1000)
                .ReceiveBufferSize(20000020)
                .SendBufferSize(20000020)
                .Build();

            await _client.ConnectAsync(clientTokenSrc.Token);

            var bytesSend = new byte[35000];
            for (var i = 0; i < bytesSend.Length; i++)
            {
                bytesSend[i] = (byte)(i % 256);
            }
            
            Log.Information("Sending data length {dataLength}", bytesSend.Length);
            
            await _client.SendAsync(bytesSend);*/
            await Task.Delay(5000);
            
            Console.ReadLine();
            //_client.Disconnect();
            await Task.Delay(3000);
        }
    }
}