using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using NetX.AutoServiceGenerator.Definitions;
using Xunit;

namespace NetX.AutoService.Tests
{
    /// <summary>
    /// Regression coverage for the nested-duplex deadlock: a dispatcher handling an inbound call must be
    /// able to call back into its own peer on the *same* connection (and await that reply) before it
    /// finishes handling the call it is currently processing. Both <c>AutoServiceServerProcessor</c> and
    /// <c>AutoServiceClientProcessor</c> used to await <c>HandleInboundAsync</c> directly from
    /// <c>OnReceivedMessageAsync</c>, which NetX's own read loop (<c>NetXConnection.ReadPipeAsync</c>)
    /// awaits before reading the next frame -- so the nested call's reply, which can only arrive via that
    /// same read loop, could never be read until the outer dispatch finished waiting for it. Fixed by
    /// starting <c>HandleInboundAsync</c> detached from the read loop in both processors.
    /// </summary>
    public class NestedDuplexDispatchTests
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

        // ---------------------------------------------------------------
        // Server side: while dispatching a forward call from the client, the server's own dispatcher
        // calls back into the client (a reverse operation) on the exact same connection and awaits the
        // reply before completing the forward call. Also proves authentication works in both directions
        // on that one connection: the client authenticates to the server before the forward call, and the
        // server authenticates to the client (via the reverse op-0 handshake) before the reverse call.
        // ---------------------------------------------------------------
        [Fact]
        public async Task Server_NestedReverseCallDuringForwardDispatch_CompletesWithoutDeadlock_AndAuthenticatesBothDirections()
        {
            const string forwardServiceId = "forward.nested.v1";
            const string reverseServiceId = "reverse.nested.v1";
            var forwardContract = TestSupport.BuildContract(forwardServiceId, 1, (1, "Forward", false));
            var reverseContract = TestSupport.BuildContract(reverseServiceId, 1, (1, "Reverse", false));

            var clientToServerCredential = Encoding.UTF8.GetBytes("client-to-server");
            var serverToClientCredential = Encoding.UTF8.GetBytes("server-to-client");

            var reverseDispatched = false;
            IAutoServicePeerSession reverseObservedSession = null;

            var reverseDispatcher = new DelegateDispatcher((request, _) =>
            {
                reverseDispatched = true;
                reverseObservedSession = AutoServiceSessionContext.Current;
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(Encoding.UTF8.GetBytes("reverse-ok")));
            });

            var forwardDispatcher = new DelegateDispatcher(async (request, ct) =>
            {
                // The peer session for the connection the forward call arrived on -- calling back into
                // it, from inside this still-in-flight dispatch, is exactly the nested pattern that used
                // to deadlock.
                var session = AutoServiceSessionContext.Current;
                Assert.NotNull(session);

                var authResponse = await session.AuthenticateAsync(serverToClientCredential, ct);
                Assert.True(authResponse.IsSuccess);

                var reverseResponse = await session.InvokeAsync(
                    reverseServiceId, 1, reverseContract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty, ct);
                Assert.True(reverseResponse.IsSuccess);
                Assert.Equal("reverse-ok", Encoding.UTF8.GetString(reverseResponse.Payload.Span));

                return AutoServiceResponse.Success(Encoding.UTF8.GetBytes("forward-ok"));
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(forwardContract, forwardDispatcher)
                .WithAuthenticator(new FixedCredentialAuthenticator(clientToServerCredential))
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(reverseContract, reverseDispatcher)
                .WithAuthenticator(new FixedCredentialAuthenticator(serverToClientCredential))
                .ConnectAsync();

            var clientAuth = await connection.AuthenticateAsync(clientToServerCredential);
            Assert.True(clientAuth.IsSuccess);

            var forwardTask = connection.Peer.InvokeAsync(
                forwardServiceId, 1, forwardContract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty).AsTask();

            // Under the pre-fix blocking read loop this never completes and the test times out here.
            var completed = await Task.WhenAny(forwardTask, Task.Delay(Timeout));
            Assert.Same(forwardTask, completed);

            var forwardResponse = await forwardTask;
            Assert.True(forwardResponse.IsSuccess);
            Assert.Equal("forward-ok", Encoding.UTF8.GetString(forwardResponse.Payload.Span));

            Assert.True(reverseDispatched);
            Assert.NotNull(reverseObservedSession);
            Assert.Same(connection.Peer, reverseObservedSession);
        }

        // ---------------------------------------------------------------
        // Client side (the necessary symmetry): while dispatching a reverse call from the server, the
        // client's own dispatcher calls back into the server (a forward operation) on the exact same
        // connection and awaits the reply before completing the reverse call. Exercises
        // AutoServiceClientProcessor's half of the same fix.
        // ---------------------------------------------------------------
        [Fact]
        public async Task Client_NestedForwardCallDuringReverseDispatch_CompletesWithoutDeadlock()
        {
            const string forwardServiceId = "forward.nested2.v1";
            const string reverseServiceId = "reverse.nested2.v1";
            var forwardContract = TestSupport.BuildContract(forwardServiceId, 1, (1, "Forward", false));
            var reverseContract = TestSupport.BuildContract(reverseServiceId, 1, (1, "Reverse", false));

            var forwardDispatched = false;
            var forwardDispatcher = new DelegateDispatcher((request, _) =>
            {
                forwardDispatched = true;
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(Encoding.UTF8.GetBytes("forward-ok")));
            });

            var reverseDispatcher = new DelegateDispatcher(async (request, ct) =>
            {
                // The peer session for the connection the reverse call arrived on (client-side).
                var session = AutoServiceSessionContext.Current;
                Assert.NotNull(session);

                var forwardResponse = await session.InvokeAsync(
                    forwardServiceId, 1, forwardContract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty, ct);
                Assert.True(forwardResponse.IsSuccess);
                Assert.Equal("forward-ok", Encoding.UTF8.GetString(forwardResponse.Payload.Span));

                return AutoServiceResponse.Success(Encoding.UTF8.GetBytes("reverse-ok"));
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(forwardContract, forwardDispatcher)
                .OnSessionConnected(async (session, ct) =>
                    await session.AuthenticateAsync(ReadOnlyMemory<byte>.Empty, ct))
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(reverseContract, reverseDispatcher)
                .ConnectAsync();

            await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

            var serverSession = host.GetAllSessions().Single();
            var reverseTask = serverSession.InvokeAsync(
                reverseServiceId, 1, reverseContract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty).AsTask();

            // Under the pre-fix blocking read loop this never completes and the test times out here.
            var completed = await Task.WhenAny(reverseTask, Task.Delay(Timeout));
            Assert.Same(reverseTask, completed);

            var reverseResponse = await reverseTask;
            Assert.True(
                reverseResponse.IsSuccess,
                $"{reverseResponse.Error?.Code}: {reverseResponse.Error?.Message}");
            Assert.Equal("reverse-ok", Encoding.UTF8.GetString(reverseResponse.Payload.Span));
            Assert.True(forwardDispatched);
        }

        // ---------------------------------------------------------------
        // Ordering: the per-peer inbound queue that unblocks the nested round trip above must still
        // process frames one at a time in arrival order. Fires the op-0 auth handshake and a follow-up
        // call back-to-back -- neither awaited before the other is sent -- so both frames can reach the
        // server before either is handled. If they were ever dispatched concurrently (or reordered)
        // instead of drained serially, the follow-up could be dispatched before authentication actually
        // applied and fail (or the pair could hang).
        // ---------------------------------------------------------------
        [Fact]
        public async Task Authentication_And_FollowUpCall_SentBackToBack_AreProcessedInOrder()
        {
            const string serviceId = "ordered.consecutive.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Echo", false));
            var credential = Encoding.UTF8.GetBytes("order-secret");

            var dispatcher = new DelegateDispatcher((request, _) =>
                new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(request.Payload)));

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(contract, dispatcher)
                .WithAuthenticator(new FixedCredentialAuthenticator(credential))
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            var authTask = connection.AuthenticateAsync(credential);
            var callTask = connection.Peer.InvokeAsync(
                serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("payload")).AsTask();

            var both = Task.WhenAll(authTask, callTask);
            var completed = await Task.WhenAny(both, Task.Delay(Timeout));
            Assert.Same(both, completed);

            var authResponse = await authTask;
            var callResponse = await callTask;
            Assert.True(authResponse.IsSuccess);
            Assert.True(callResponse.IsSuccess);
            Assert.Equal("payload", Encoding.UTF8.GetString(callResponse.Payload.Span));
        }
    }
}
