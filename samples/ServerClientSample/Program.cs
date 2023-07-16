using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NetX;
using NetX.Options;
using Serilog;
using static System.Net.Mime.MediaTypeNames;

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

            var responses = new bool[byte.MaxValue];

            for (byte i = 1; i < byte.MaxValue; i++)
            {
                byte value = i;
                _ = Task.Run(async () =>
                {
                    try
                    {
                        var respon = await _client.RequestAsync(new byte[] { value });
                        responses[value] = true;
                        await Task.Delay(100);
                        Log.Information("Received delayed response: {resp}", respon.Array);
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "Error");
                    }
                });
            }

            //_ = Task.Run(async () =>
            //{
            //    while (!serverTokenSrc.IsCancellationRequested)
            //    {
            //        if (!responses.All(x => x))
            //        {
            //            Log.Information("Waiting for responses");
            //            await Task.Delay(1000);
            //        }
            //        else
            //        {
            //            Log.Information("All responses received");
            //            break;
            //        }
            //    }
            //});

            try
            {
                await _client.RequestAsync(new byte[] { 0 });
            }
            catch
            {
                responses[0] = true;
                Log.Information("Success timeout error");
            }

            bool stopAll = false;
            while (!stopAll)
            {
                var command = Console.ReadLine();

                if (command == "stop_client")
                {
                    Console.WriteLine("Stopping client");
                    clientTokenSrc.Cancel();
                }

                if (command == "stop_server")
                {
                    Console.WriteLine("Stopping server");
                    serverTokenSrc.Cancel();
                }

                if (command == "stop")
                {
                    stopAll = true;
                }

                await Task.Yield();
            }

            await Task.Delay(3000);
        }
    }
}