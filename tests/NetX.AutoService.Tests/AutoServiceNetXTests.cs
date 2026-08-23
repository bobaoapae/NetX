using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetX;
using NetX.AutoService;
using NetX.AutoServiceGenerator.Definitions;
using Xunit;

namespace NetX.AutoService.Tests
{
    public class AutoServiceNetXTests
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

        // ---------------------------------------------------------------
        // 1. Basic request/response round-trip.
        // ---------------------------------------------------------------
        [Fact]
        public async Task RequestResponse_RoundTrips_ThroughRegisteredService()
        {
            const string serviceId = "echo.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Echo", false));
            var dispatcher = new DelegateDispatcher((request, _) =>
            {
                var upper = Encoding.UTF8.GetString(request.Payload.Span).ToUpperInvariant();
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(Encoding.UTF8.GetBytes(upper)));
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(contract, dispatcher)
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            var auth = await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);
            Assert.True(auth.IsSuccess);

            var response = await connection.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("hello"));

            Assert.True(response.IsSuccess);
            Assert.Equal("HELLO", Encoding.UTF8.GetString(response.Payload.Span));
        }

        // ---------------------------------------------------------------
        // 2. One-way (fire-and-forget) delivery.
        // ---------------------------------------------------------------
        [Fact]
        public async Task OneWay_Call_IsDelivered()
        {
            const string serviceId = "notify.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Notify", true));
            var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var dispatcher = new DelegateDispatcher((request, _) =>
            {
                received.TrySetResult(Encoding.UTF8.GetString(request.Payload.Span));
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(ReadOnlyMemory<byte>.Empty));
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(contract, dispatcher)
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

            await connection.Peer.SendOneWayAsync(serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("ping"));

            var completed = await Task.WhenAny(received.Task, Task.Delay(Timeout));
            Assert.Same(received.Task, completed);
            Assert.Equal("ping", await received.Task);
        }

        // ---------------------------------------------------------------
        // 3. Auth handshake success/failure and Principal visibility.
        // ---------------------------------------------------------------
        [Fact]
        public async Task Authentication_Succeeds_And_ExposesPrincipal_OnServerSession()
        {
            var credential = Encoding.UTF8.GetBytes("letmein");
            var port = TestSupport.GetFreeTcpPort();

            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithAuthenticator(() => new FixedCredentialAuthenticator(credential))
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            var response = await connection.AuthenticateAsync(credential);
            Assert.True(response.IsSuccess);

            // Give the server-side session dictionary a moment to reflect state set inside DispatchAsync
            // (the reply we just awaited only proves the server already processed it).
            var serverSession = host.GetAllSessions().Single();
            Assert.True(serverSession.IsAuthenticated);
            Assert.Equal("letmein", serverSession.Principal);
        }

        [Fact]
        public async Task Authentication_Fails_ForWrongCredential_AndBlocksSubsequentCalls()
        {
            var credential = Encoding.UTF8.GetBytes("letmein");
            const string serviceId = "echo.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Echo", false));
            var dispatcher = new DelegateDispatcher((request, _) =>
                new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(request.Payload)));

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithAuthenticator(() => new FixedCredentialAuthenticator(credential))
                .WithService<object>(contract, dispatcher)
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            var authResponse = await connection.AuthenticateAsync(Encoding.UTF8.GetBytes("wrong"));
            Assert.False(authResponse.IsSuccess);
            Assert.Equal(AutoServiceErrorCode.Unauthenticated, authResponse.Error.Code);

            var callResponse = await connection.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("x"));
            Assert.False(callResponse.IsSuccess);
            Assert.Equal(AutoServiceErrorCode.Unauthenticated, callResponse.Error.Code);
        }

        // ---------------------------------------------------------------
        // 4. Simultaneous reverse/duplex calls, correlation-safe both ways.
        // ---------------------------------------------------------------
        [Fact]
        public async Task Duplex_ConcurrentCallsInBothDirections_DoNotCrossTalk()
        {
            const string mathServiceId = "math.v1";
            const string notifyServiceId = "notify.v1";
            var mathContract = TestSupport.BuildContract(mathServiceId, 1, (1, "Add", false));
            var notifyContract = TestSupport.BuildContract(notifyServiceId, 1, (1, "Notify", false));
            var credential = ReadOnlyMemory<byte>.Empty;

            // Server-side: adds two little-endian int32s from the payload.
            var mathDispatcher = new DelegateDispatcher((request, _) =>
            {
                var a = BinaryPrimitives.ReadInt32LittleEndian(request.Payload.Span);
                var b = BinaryPrimitives.ReadInt32LittleEndian(request.Payload.Span[4..]);
                var result = new byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(result, a + b);
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(result));
            });

            // Client-side (reverse): echoes back the int32 it was notified with, doubled.
            var notifyDispatcher = new DelegateDispatcher((request, _) =>
            {
                var value = BinaryPrimitives.ReadInt32LittleEndian(request.Payload.Span);
                var result = new byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(result, value * 2);
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(result));
            });

            var port = TestSupport.GetFreeTcpPort();

            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(mathContract, mathDispatcher)
                .OnSessionConnected(async (session, ct) =>
                {
                    // Authenticate the reverse direction too: the client's own dispatcher (which guards
                    // the reverse NotifyService) needs its own op-0 handshake before the server can call in.
                    await session.AuthenticateAsync(credential, ct);
                })
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(notifyContract, notifyDispatcher)
                .ConnectAsync();

            await connection.AuthenticateAsync(credential);

            const int callCount = 20;
            var forwardTasks = Enumerable.Range(0, callCount).Select(async i =>
            {
                var payload = new byte[8];
                BinaryPrimitives.WriteInt32LittleEndian(payload, i);
                BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(4), 1000);
                var response = await connection.Peer.InvokeAsync(mathServiceId, 1, mathContract.Fingerprint, 1, payload);
                Assert.True(response.IsSuccess);
                var sum = BinaryPrimitives.ReadInt32LittleEndian(response.Payload.Span);
                Assert.Equal(i + 1000, sum);
            }).ToArray();

            var serverSession = host.GetAllSessions().Single();
            var reverseTasks = Enumerable.Range(0, callCount).Select(async i =>
            {
                var payload = new byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(payload, i);
                var response = await serverSession.InvokeAsync(notifyServiceId, 1, notifyContract.Fingerprint, 1, payload);
                Assert.True(response.IsSuccess);
                var doubled = BinaryPrimitives.ReadInt32LittleEndian(response.Payload.Span);
                Assert.Equal(i * 2, doubled);
            }).ToArray();

            await Task.WhenAll(forwardTasks.Concat(reverseTasks));
        }

        // ---------------------------------------------------------------
        // 5. MaxFrameBytes boundary behavior.
        // ---------------------------------------------------------------
        [Fact]
        public async Task Payload_UnderMaxFrameBytes_RoundTrips_And_OverLimit_IsRejectedCleanly()
        {
            const string serviceId = "blob.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Store", false));
            var dispatcher = new DelegateDispatcher((request, _) =>
                new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(BitConverter.GetBytes(request.Payload.Length))));

            const int maxFrameBytes = 4 * 1024 * 1024;
            var port = TestSupport.GetFreeTcpPort();

            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithMaxFrameBytes(maxFrameBytes)
                .WithService<object>(contract, dispatcher)
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithMaxFrameBytes(maxFrameBytes)
                .ConnectAsync();

            await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

            // Comfortably under the limit once the AutoServiceFrame header/envelope overhead is accounted for.
            var underLimitPayload = new byte[maxFrameBytes - 4096];
            new Random(42).NextBytes(underLimitPayload);

            var okResponse = await connection.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, underLimitPayload);
            Assert.True(okResponse.IsSuccess);
            Assert.Equal(underLimitPayload.Length, BitConverter.ToInt32(okResponse.Payload.Span));

            // Clearly over the limit: must fail cleanly (a faulted/failed response), not hang or corrupt the connection.
            var overLimitPayload = new byte[maxFrameBytes * 2];
            var overLimitResponse = await connection.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, overLimitPayload);
            Assert.False(overLimitResponse.IsSuccess);

            // The connection must still be usable afterwards.
            var followUpPayload = new byte[1024];
            var followUpResponse = await connection.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, followUpPayload);
            Assert.True(followUpResponse.IsSuccess);
        }
    }
}
