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
        private static CancellationToken _token;

        public static async Task Main(string[] args)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            _token = cancellationTokenSource.Token;

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

            _server.Listen(cancellationTokenSource.Token);

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

            await _client.ConnectAsync(cancellationTokenSource.Token);

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

            _ = Task.Run(async () =>
            {
                while (!cancellationTokenSource.IsCancellationRequested)
                {
                    if (!responses.All(x => x))
                    {
                        Log.Information("Waiting for responses");
                        await Task.Delay(1000);
                    }
                    else
                    {
                        Log.Information("All responses received");
                        break;
                    }
                }
            });

            var response = await _client.RequestAsync(new byte[] { 255 });
            Log.Information("Response request: {resp}", response);

            try
            {
                await _client.RequestAsync(new byte[] { 0 });
            }
            catch (Exception ex)
            {
                responses[0] = true;
                Log.Error(ex, "Success timeout error");
            }

            while (!cancellationTokenSource.IsCancellationRequested)
            {
                var command = Console.ReadLine();

                if (command == "stop")
                {
                    Console.WriteLine("Stopping");
                    cancellationTokenSource.Cancel();
                }

                await Task.Yield();
            }

            await Task.Delay(3000);
        }

        private static async Task WorkerClient()
        {
            try
            {
                while (!_token.IsCancellationRequested)
                {
                    //Console.WriteLine($"Received Response: {response.Array[0]}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }
}