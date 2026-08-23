using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetX;
using NetX.AutoService;
using NetX.AutoServiceGenerator.Definitions;
using Xunit;

namespace NetX.AutoService.Tests
{
    /// <summary>
    /// Focused regression tests for the six defects fixed in the binding review: listener
    /// shutdown/port reuse, over-limit payload handling (structured error vs. explicit
    /// disconnect), MaxFrameBytes overhead-margin overflow, and connect/disconnect callback
    /// ordering. Request-vs-OneWay kind mismatch is covered by <see cref="AutoServiceNetXTests"/>
    /// indirectly and here directly against both mismatch directions.
    /// </summary>
    public class BindingReviewFixesTests
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

        // ---------------------------------------------------------------
        // 1. Stop()/Dispose() actually closes the listening socket: the port can be rebound.
        // ---------------------------------------------------------------
        [Fact]
        public async Task Dispose_StopsListener_AndPortCanBeReboundAfterward()
        {
            var port = TestSupport.GetFreeTcpPort();

            var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .StartAsync();

            using (var connection = await AutoServiceNetXClient.Create()
                       .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                       .ConnectAsync())
            {
                var auth = await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);
                Assert.True(auth.IsSuccess);
            }

            host.Dispose();

            // The listening socket must be fully closed -- a brand new listener can bind the exact
            // same port immediately afterward. This would throw SocketException("address already in
            // use") if Dispose()/Stop() left the old socket open.
            using var rebound = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .StartAsync();

            using var reconnect = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            var reauth = await reconnect.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);
            Assert.True(reauth.IsSuccess);
        }

        // ---------------------------------------------------------------
        // 2. Over-limit but decodable request: structured error reply, not a silent timeout.
        // ---------------------------------------------------------------
        [Fact]
        public async Task Request_ExceedingServerAutoServiceLimit_GetsStructuredError_NotSilentTimeout()
        {
            const string serviceId = "blob.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Store", false));
            var dispatched = false;
            var dispatcher = new DelegateDispatcher((request, _) =>
            {
                dispatched = true;
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(ReadOnlyMemory<byte>.Empty));
            });

            const int serverMaxFrameBytes = 2048;
            var port = TestSupport.GetFreeTcpPort();

            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithMaxFrameBytes(serverMaxFrameBytes)
                .WithService<object>(contract, dispatcher)
                .StartAsync();

            // Client allows much larger frames than the server: NetX's own transport-level guard on
            // the server (serverMaxFrameBytes + overhead margin) still lets the frame arrive, but the
            // AutoService-level codec on the server rejects it as over its own configured limit.
            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

            var oversizedPayload = new byte[serverMaxFrameBytes + 1024];
            new Random(7).NextBytes(oversizedPayload);

            var responseTask = connection.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, oversizedPayload);
            var completed = await Task.WhenAny(responseTask.AsTask(), Task.Delay(Timeout));

            Assert.Same(responseTask.AsTask(), completed);
            var response = await responseTask;
            Assert.False(response.IsSuccess);
            Assert.Equal(AutoServiceErrorCode.InvalidRequest, response.Error.Code);
            Assert.False(dispatched);

            // The connection must still be usable afterwards -- the oversized request must not have
            // corrupted the session.
            var followUp = await connection.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, new byte[16]);
            Assert.True(followUp.IsSuccess);
        }

        // ---------------------------------------------------------------
        // 2b. Over-limit and non-repliable (OneWay): explicit disconnect, not a silent drop.
        // ---------------------------------------------------------------
        [Fact]
        public async Task OneWay_ExceedingServerAutoServiceLimit_DisconnectsExplicitly_NotSilentDrop()
        {
            const string serviceId = "notify.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Notify", true));
            var dispatched = false;
            var dispatcher = new DelegateDispatcher((request, _) =>
            {
                dispatched = true;
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(ReadOnlyMemory<byte>.Empty));
            });

            const int serverMaxFrameBytes = 2048;
            var port = TestSupport.GetFreeTcpPort();

            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithMaxFrameBytes(serverMaxFrameBytes)
                .WithService<object>(contract, dispatcher)
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

            var oversizedPayload = new byte[serverMaxFrameBytes + 1024];
            new Random(11).NextBytes(oversizedPayload);

            await connection.Peer.SendOneWayAsync(serviceId, 1, contract.Fingerprint, 1, oversizedPayload);

            // No reply channel exists for a OneWay frame, so the server must tear the connection down
            // as a protocol violation instead of leaving the client to eventually time out believing
            // delivery may have succeeded.
            var deadline = DateTime.UtcNow + Timeout;
            while (connection.NetXClient.IsConnected && DateTime.UtcNow < deadline)
                await Task.Delay(50);

            Assert.False(connection.NetXClient.IsConnected);
            Assert.False(dispatched);
        }

        // ---------------------------------------------------------------
        // 3. MaxFrameBytes + overhead-margin arithmetic must fail closed instead of overflowing.
        // ---------------------------------------------------------------
        [Fact]
        public async Task WithMaxFrameBytes_NearIntMax_FailsClosed_InsteadOfOverflowing()
        {
            var nearIntMax = int.MaxValue - 100;

            // AutoServiceNetXServerBuilder.StartAsync is synchronous up to (and including) the
            // overflowing arithmetic, so the guard throws synchronously rather than via a faulted Task
            // -- still routed through ThrowsAsync since StartAsync's return type is a Task.
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                AutoServiceNetXServer.Create()
                    .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)TestSupport.GetFreeTcpPort())
                    .WithMaxFrameBytes(nearIntMax)
                    .StartAsync());

            // AutoServiceNetXClientBuilder.ConnectAsync is an async method: the same guard runs in its
            // synchronous prefix, but the exception surfaces on the returned (faulted) Task instead of
            // synchronously out of the call.
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                AutoServiceNetXClient.Create()
                    .WithEndPoint(IPAddress.Loopback.ToString(), 1)
                    .WithMaxFrameBytes(nearIntMax)
                    .ConnectAsync());
        }

        // ---------------------------------------------------------------
        // 4. Request sent to a declared-OneWay operation, and OneWay sent to a declared-request
        //    operation, are both rejected clearly instead of dispatching ambiguously.
        // ---------------------------------------------------------------
        [Fact]
        public async Task KindMismatch_BetweenRequestAndOneWay_IsRejectedClearly_InBothDirections()
        {
            const string oneWayServiceId = "onewayonly.v1";
            const string requestServiceId = "requestonly.v1";
            var oneWayContract = TestSupport.BuildContract(oneWayServiceId, 1, (1, "FireAndForget", true));
            var requestContract = TestSupport.BuildContract(requestServiceId, 1, (1, "MustReply", false));

            var oneWayDispatched = false;
            var requestDispatched = false;
            var oneWayDispatcher = new DelegateDispatcher((request, _) =>
            {
                oneWayDispatched = true;
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(ReadOnlyMemory<byte>.Empty));
            });
            var requestDispatcher = new DelegateDispatcher((request, _) =>
            {
                requestDispatched = true;
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(ReadOnlyMemory<byte>.Empty));
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(oneWayContract, oneWayDispatcher)
                .WithService<object>(requestContract, requestDispatcher)
                .StartAsync();

            // Direction A: a Request frame (expects a reply) against a declared-OneWay operation --
            // there IS a reply channel, so the server must answer with a structured error rather than
            // dispatching or hanging.
            using (var connection = await AutoServiceNetXClient.Create()
                       .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                       .ConnectAsync())
            {
                await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

                var responseTask = connection.Peer.InvokeAsync(oneWayServiceId, 1, oneWayContract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty);
                var completed = await Task.WhenAny(responseTask.AsTask(), Task.Delay(Timeout));
                Assert.Same(responseTask.AsTask(), completed);

                var response = await responseTask;
                Assert.False(response.IsSuccess);
                Assert.Equal(AutoServiceErrorCode.InvalidRequest, response.Error.Code);
                Assert.False(oneWayDispatched);
            }

            // Direction B: a OneWay frame (no reply channel) against a declared-request operation --
            // the server cannot explain itself back, so it must disconnect explicitly instead of
            // silently dropping the frame and leaving the sender to assume delivery succeeded.
            using (var connection = await AutoServiceNetXClient.Create()
                       .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                       .ConnectAsync())
            {
                await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

                await connection.Peer.SendOneWayAsync(requestServiceId, 1, requestContract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty);

                var deadline = DateTime.UtcNow + Timeout;
                while (connection.NetXClient.IsConnected && DateTime.UtcNow < deadline)
                    await Task.Delay(50);

                Assert.False(connection.NetXClient.IsConnected);
                Assert.False(requestDispatched);
            }
        }

        // ---------------------------------------------------------------
        // 5. OnSessionConnectAsync must fully complete before OnSessionDisconnectAsync fires for the
        //    same session -- never inverted or interleaved.
        // ---------------------------------------------------------------
        [Fact]
        public async Task SessionCallbacks_ConnectAlwaysCompletesBeforeDisconnect_ForSameSession()
        {
            var events = new ConcurrentQueue<string>();
            var connectStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseConnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .OnSessionConnected(async (session, ct) =>
                {
                    events.Enqueue("connect-start");
                    connectStarted.TrySetResult();
                    await releaseConnect.Task;
                    events.Enqueue("connect-end");
                })
                .OnSessionDisconnected((session, reason) =>
                {
                    events.Enqueue("disconnect");
                    disconnected.TrySetResult();
                    return ValueTask.CompletedTask;
                })
                .StartAsync();

            var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            var startedCompleted = await Task.WhenAny(connectStarted.Task, Task.Delay(Timeout));
            Assert.Same(connectStarted.Task, startedCompleted);

            // Tear the connection down while the connect callback is still deliberately blocked.
            connection.Dispose();
            await Task.Delay(200);

            // The disconnect callback must not have fired yet -- the connect callback for the same
            // session has not finished.
            Assert.DoesNotContain("disconnect", events);

            releaseConnect.TrySetResult();

            var disconnectCompleted = await Task.WhenAny(disconnected.Task, Task.Delay(Timeout));
            Assert.Same(disconnected.Task, disconnectCompleted);

            Assert.Equal(new[] { "connect-start", "connect-end", "disconnect" }, events.ToArray());
        }
    }
}
