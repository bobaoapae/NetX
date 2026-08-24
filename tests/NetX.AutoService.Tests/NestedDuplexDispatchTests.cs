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
    /// finishes handling the call it is currently processing. Inbound decode and routing are drained
    /// from a per-peer queue; operation 0 remains an inline authentication barrier, while non-auth
    /// dispatches run concurrently within the configured per-peer slot limit. This lets a nested call's
    /// reply reach the same NetX read loop even while the outer dispatch is waiting, without promising
    /// completion ordering between non-auth operations.
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
        // Mutual depth-two reentrancy: A(server) -> B(client) -> C(server). The second server-side
        // dispatch must not wait behind A on the same peer. The timeout is deliberately outside the
        // handlers so the pre-fix deadlock is observed as a failed convergence assertion, while the
        // cleanup path can still release the blocked outer request.
        // ---------------------------------------------------------------
        [Fact]
        public async Task MutualNestedDuplex_DepthTwo_CompletesOnTheSameSession()
        {
            const string outerServiceId = "mutual.depth2.outer.v1";
            const string innerClientServiceId = "mutual.depth2.client.v1";
            const string innerServerServiceId = "mutual.depth2.inner.v1";
            var outerContract = TestSupport.BuildContract(outerServiceId, 1, (1, "Outer", false));
            var innerClientContract = TestSupport.BuildContract(innerClientServiceId, 1, (1, "Client", false));
            var innerServerContract = TestSupport.BuildContract(innerServerServiceId, 1, (1, "Inner", false));

            var serverSession = (IAutoServicePeerSession)null;
            var clientHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var serverInnerDispatched = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var innerServerDispatcher = new DelegateDispatcher((request, _) =>
            {
                Assert.Same(serverSession, AutoServiceSessionContext.Current);
                serverInnerDispatched.TrySetResult();
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(Encoding.UTF8.GetBytes("c-ok")));
            });

            var clientDispatcher = new DelegateDispatcher(async (request, ct) =>
            {
                var session = AutoServiceSessionContext.Current;
                Assert.NotNull(session);
                clientHandlerStarted.TrySetResult();

                var response = await session.InvokeAsync(
                    innerServerServiceId, 1, innerServerContract.Fingerprint, 1,
                    ReadOnlyMemory<byte>.Empty, ct);
                Assert.True(response.IsSuccess, response.Error?.Message);
                Assert.Equal("c-ok", Encoding.UTF8.GetString(response.Payload.Span));
                return AutoServiceResponse.Success(Encoding.UTF8.GetBytes("b-ok"));
            });

            var outerDispatcher = new DelegateDispatcher(async (request, ct) =>
            {
                var session = AutoServiceSessionContext.Current;
                Assert.Same(serverSession, session);

                var response = await session.InvokeAsync(
                    innerClientServiceId, 1, innerClientContract.Fingerprint, 1,
                    ReadOnlyMemory<byte>.Empty, ct);
                Assert.True(response.IsSuccess, response.Error?.Message);
                Assert.Equal("b-ok", Encoding.UTF8.GetString(response.Payload.Span));
                return AutoServiceResponse.Success(Encoding.UTF8.GetBytes("a-ok"));
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(outerContract, outerDispatcher)
                .WithService<object>(innerServerContract, innerServerDispatcher)
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(innerClientContract, clientDispatcher)
                .ConnectAsync();

            serverSession = host.GetAllSessions().Single();
            var clientAuth = await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);
            var serverAuth = await serverSession.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);
            Assert.True(clientAuth.IsSuccess);
            Assert.True(serverAuth.IsSuccess);

            var outerTask = connection.Peer.InvokeAsync(
                outerServiceId, 1, outerContract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty).AsTask();

            var completed = await Task.WhenAny(outerTask, Task.Delay(Timeout));
            if (!ReferenceEquals(completed, outerTask))
            {
                // Let the blocked path unwind before the assertion fails, avoiding a background
                // dispatcher left behind by the expected pre-fix deadlock.
                connection.Peer.Disconnect();
                try { await outerTask; } catch { }
            }

            Assert.Same(outerTask, completed);
            var outerResponse = await outerTask;
            Assert.True(outerResponse.IsSuccess, outerResponse.Error?.Message);
            Assert.Equal("a-ok", Encoding.UTF8.GetString(outerResponse.Payload.Span));
            Assert.True(clientHandlerStarted.Task.IsCompletedSuccessfully);
            Assert.True(serverInnerDispatched.Task.IsCompletedSuccessfully);
        }

        // ---------------------------------------------------------------
        // Two non-auth requests on one peer must overlap. Request 1 waits for request 2 to enter its
        // handler; request 2 releases request 1. Each dispatch also records the ambient session, which
        // must remain the exact server-side peer rather than leaking/nulling across detached tasks.
        // ---------------------------------------------------------------
        [Fact]
        public async Task SamePeer_ConcurrentRequests_Overlap_AndPreserveSessionContext()
        {
            const string serviceId = "same.peer.concurrent.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Echo", false));
            var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var allowFirstCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var observedSessions = new System.Collections.Concurrent.ConcurrentBag<IAutoServicePeerSession>();
            var serverSession = (IAutoServicePeerSession)null;

            var dispatcher = new DelegateDispatcher(async (request, _) =>
            {
                var session = AutoServiceSessionContext.Current;
                Assert.NotNull(session);
                Assert.Same(serverSession, session);
                observedSessions.Add(session);

                var marker = Encoding.UTF8.GetString(request.Payload.Span);
                if (marker == "first")
                {
                    firstStarted.TrySetResult();
                    await Task.WhenAny(secondStarted.Task, allowFirstCompletion.Task);
                }
                else
                {
                    secondStarted.TrySetResult();
                    allowFirstCompletion.TrySetResult();
                }

                return AutoServiceResponse.Success(request.Payload);
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(contract, dispatcher)
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            serverSession = host.GetAllSessions().Single();
            var authenticated = await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);
            Assert.True(authenticated.IsSuccess);

            var firstTask = connection.Peer.InvokeAsync(
                serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("first")).AsTask();
            await firstStarted.Task.WaitAsync(Timeout);
            var secondTask = connection.Peer.InvokeAsync(
                serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("second")).AsTask();
            var all = Task.WhenAll(firstTask, secondTask);

            try
            {
                var completed = await Task.WhenAny(all, Task.Delay(Timeout));
                Assert.Same(all, completed);

                var responses = await all;
                Assert.All(responses, response => Assert.True(response.IsSuccess, response.Error?.Message));
                Assert.Equal(2, observedSessions.Count);
                Assert.All(observedSessions, session => Assert.Same(serverSession, session));
            }
            finally
            {
                // A regressed serial worker can let the client's duplex timeout complete `all`
                // before request 2 ever enters its handler. Always release request 1 so that failure
                // cannot strand a background dispatch after the test has reported the regression.
                allowFirstCompletion.TrySetResult();
            }
        }

        // ---------------------------------------------------------------
        // Ordering barrier: decode/admission remain FIFO, but only operation 0 is dispatched inline.
        // Fire auth and a follow-up back-to-back so both can reach the server before either is handled;
        // the worker must await auth before scheduling the concurrent non-auth path. Completion order
        // among already-authenticated non-auth operations is deliberately not promised.
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
