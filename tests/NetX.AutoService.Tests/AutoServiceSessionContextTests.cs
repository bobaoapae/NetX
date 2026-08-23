using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetX.AutoServiceGenerator.Definitions;
using Xunit;

namespace NetX.AutoService.Tests
{
    /// <summary>
    /// Covers <see cref="AutoServiceSessionContext"/> (the AsyncLocal session identity set around every
    /// inbound dispatch) and the session-aware <c>WithAuthenticator</c> overloads added alongside it.
    /// </summary>
    public class AutoServiceSessionContextTests
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

        // ---------------------------------------------------------------
        // Current is null outside of any dispatch.
        // ---------------------------------------------------------------
        [Fact]
        public void Current_IsNull_OutsideOfDispatch()
        {
            Assert.Null(AutoServiceSessionContext.Current);
        }

        // ---------------------------------------------------------------
        // Concurrent multi-session dispatch: each call observes its own session, not some other
        // connection's -- the exact hole the previous shared-router binding had (tests relying on
        // GetAllSessions().Single()). Both handlers are gated so neither can complete before both have
        // started, forcing genuine async overlap rather than two calls that merely start close together.
        // ---------------------------------------------------------------
        [Fact]
        public async Task Current_DuringDispatch_IsTheCallersOwnSession_AcrossConcurrentSessions()
        {
            const string serviceId = "whoami.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "WhoAmI", false));
            var observed = new ConcurrentDictionary<string, Guid>();
            var bothStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var startedCount = 0;

            var dispatcher = new DelegateDispatcher(async (request, _) =>
            {
                var marker = Encoding.UTF8.GetString(request.Payload.Span);
                var current = AutoServiceSessionContext.Current;
                Assert.NotNull(current);
                observed[marker] = current.Id;

                // Neither handler is allowed to return before both are confirmed in-flight
                // simultaneously -- proves the two dispatches genuinely overlap on the async pool
                // rather than merely being issued close together and serializing by luck.
                if (Interlocked.Increment(ref startedCount) == 2)
                    bothStarted.TrySetResult();
                await bothStarted.Task.WaitAsync(Timeout);

                return AutoServiceResponse.Success(ReadOnlyMemory<byte>.Empty);
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(contract, dispatcher)
                .StartAsync();

            using var connectionA = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();
            using var connectionB = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            await connectionA.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);
            await connectionB.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

            var callA = connectionA.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("A")).AsTask();
            var callB = connectionB.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("B")).AsTask();

            var completed = await Task.WhenAny(Task.WhenAll(callA, callB), Task.Delay(Timeout));
            Assert.True(completed is Task<AutoServiceResponse[]>);
            Assert.True((await callA).IsSuccess);
            Assert.True((await callB).IsSuccess);

            Assert.True(observed.TryGetValue("A", out var sessionAId));
            Assert.True(observed.TryGetValue("B", out var sessionBId));
            Assert.NotEqual(sessionAId, sessionBId);

            var serverSessionIds = host.GetAllSessions().Select(s => s.Id).ToHashSet();
            Assert.Contains(sessionAId, serverSessionIds);
            Assert.Contains(sessionBId, serverSessionIds);

            // The ambient value is scoped strictly to the in-flight dispatch -- it does not leak onto
            // the calling test's own async continuation once both round trips have completed.
            Assert.Null(AutoServiceSessionContext.Current);
        }

        // ---------------------------------------------------------------
        // Credential -> session mapping: the session-aware authenticator factory runs *before*
        // authentication (see AutoServicePeerSession ctor), so it cannot know which credential a peer
        // will present. This proves the two are nonetheless correctly correlated end-to-end: the
        // session captured by the factory for connection A is the same session later observed as
        // Current when A's own (distinct) credential is used to authenticate and A dispatches a call --
        // and likewise for B, with no cross-wiring between the two.
        // ---------------------------------------------------------------
        [Fact]
        public async Task SessionAwareAuthenticatorFactory_SessionMapsToItsOwnConnectionsCredential()
        {
            const string serviceId = "whoami.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "WhoAmI", false));
            var credentialA = Encoding.UTF8.GetBytes("credential-A");
            var credentialB = Encoding.UTF8.GetBytes("credential-B");

            var credentialToSessionId = new ConcurrentDictionary<string, Guid>();

            IAutoServiceStrictAuthenticator AuthenticatorFactory(IAutoServicePeerSession session)
                => new CredentialMappingAuthenticator(session, credentialToSessionId);

            var dispatcher = new DelegateDispatcher((request, _) =>
            {
                var marker = Encoding.UTF8.GetString(request.Payload.Span);
                credentialToSessionId["dispatch:" + marker] = AutoServiceSessionContext.Current.Id;
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(ReadOnlyMemory<byte>.Empty));
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(contract, dispatcher)
                .WithAuthenticator((Func<IAutoServicePeerSession, IAutoServiceStrictAuthenticator>)AuthenticatorFactory)
                .StartAsync();

            using var connectionA = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();
            using var connectionB = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            Assert.True((await connectionA.AuthenticateAsync(credentialA)).IsSuccess);
            Assert.True((await connectionB.AuthenticateAsync(credentialB)).IsSuccess);

            Assert.True((await connectionA.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("A"))).IsSuccess);
            Assert.True((await connectionB.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("B"))).IsSuccess);

            Assert.True(credentialToSessionId.TryGetValue("credential-A", out var sessionForCredentialA));
            Assert.True(credentialToSessionId.TryGetValue("credential-B", out var sessionForCredentialB));
            Assert.True(credentialToSessionId.TryGetValue("dispatch:A", out var sessionForDispatchA));
            Assert.True(credentialToSessionId.TryGetValue("dispatch:B", out var sessionForDispatchB));

            // A -> A, B -> B, and never crossed.
            Assert.Equal(sessionForCredentialA, sessionForDispatchA);
            Assert.Equal(sessionForCredentialB, sessionForDispatchB);
            Assert.NotEqual(sessionForCredentialA, sessionForCredentialB);
        }

        // ---------------------------------------------------------------
        // Current is set during the op-0 handshake itself, not just for ordinary service calls.
        // ---------------------------------------------------------------
        [Fact]
        public async Task Current_IsAvailable_DuringOp0Authentication()
        {
            var credential = Encoding.UTF8.GetBytes("letmein");
            Guid? observedDuringAuth = null;
            var port = TestSupport.GetFreeTcpPort();

            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithAuthenticator(() => new CapturingAuthenticator(credential, () => observedDuringAuth = AutoServiceSessionContext.Current?.Id))
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            var response = await connection.AuthenticateAsync(credential);
            Assert.True(response.IsSuccess);

            Assert.NotNull(observedDuringAuth);
            var serverSession = host.GetAllSessions().Single();
            Assert.Equal(serverSession.Id, observedDuringAuth);
        }

        // ---------------------------------------------------------------
        // Session-aware WithAuthenticator: the factory is handed the exact session being constructed.
        // ---------------------------------------------------------------
        [Fact]
        public async Task SessionAwareAuthenticatorFactory_ReceivesTheRightSession()
        {
            IAutoServicePeerSession connectedSession = null;
            IAutoServicePeerSession factorySession = null;
            var port = TestSupport.GetFreeTcpPort();

            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithAuthenticator((IAutoServicePeerSession session) =>
                {
                    factorySession = session;
                    return AutoServiceNoAuthAuthenticatorAdapter.Instance;
                })
                .OnSessionConnected((session, _) =>
                {
                    connectedSession = session;
                    return ValueTask.CompletedTask;
                })
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

            Assert.NotNull(factorySession);
            Assert.NotNull(connectedSession);
            Assert.Same(connectedSession, factorySession);
        }

        // ---------------------------------------------------------------
        // The client's own reverse dispatch (server calling back into the client) also sets Current.
        // ---------------------------------------------------------------
        [Fact]
        public async Task Current_IsSet_OnReverseClientDispatch()
        {
            const string notifyServiceId = "notify.v1";
            var notifyContract = TestSupport.BuildContract(notifyServiceId, 1, (1, "Notify", false));
            var credential = ReadOnlyMemory<byte>.Empty;
            IAutoServicePeerSession observedOnClient = null;

            var notifyDispatcher = new DelegateDispatcher((request, _) =>
            {
                observedOnClient = AutoServiceSessionContext.Current;
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(ReadOnlyMemory<byte>.Empty));
            });

            var port = TestSupport.GetFreeTcpPort();

            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .OnSessionConnected(async (session, ct) => await session.AuthenticateAsync(credential, ct))
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(notifyContract, notifyDispatcher)
                .ConnectAsync();

            await connection.AuthenticateAsync(credential);

            var serverSession = host.GetAllSessions().Single();
            var response = await serverSession.InvokeAsync(notifyServiceId, 1, notifyContract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty);
            Assert.True(response.IsSuccess);

            Assert.NotNull(observedOnClient);
            Assert.Same(connection.Peer, observedOnClient);
        }

        // ---------------------------------------------------------------
        // Identity is stable: the same session instance is handed to OnSessionConnected, Current during
        // dispatch, TryGetSession and OnSessionDisconnected -- ReferenceEquals keeps working as a fence key.
        // ---------------------------------------------------------------
        [Fact]
        public async Task SameSessionInstance_AcrossConnected_Dispatch_TryGetSession_AndDisconnected()
        {
            const string serviceId = "echo.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Echo", false));

            IAutoServicePeerSession connectedSession = null;
            IAutoServicePeerSession dispatchSession = null;
            IAutoServicePeerSession disconnectedSession = null;
            var disconnectedSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var dispatcher = new DelegateDispatcher((request, _) =>
            {
                dispatchSession = AutoServiceSessionContext.Current;
                return new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(ReadOnlyMemory<byte>.Empty));
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(contract, dispatcher)
                .OnSessionConnected((session, _) =>
                {
                    connectedSession = session;
                    return ValueTask.CompletedTask;
                })
                .OnSessionDisconnected((session, _) =>
                {
                    disconnectedSession = session;
                    disconnectedSignal.TrySetResult();
                    return ValueTask.CompletedTask;
                })
                .StartAsync();

            var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

            Assert.NotNull(connectedSession);
            Assert.True(host.TryGetSession(connectedSession.Id, out var lookedUpSession));
            Assert.Same(connectedSession, lookedUpSession);

            var response = await connection.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty);
            Assert.True(response.IsSuccess);
            Assert.Same(connectedSession, dispatchSession);

            connection.Dispose();
            var completed = await Task.WhenAny(disconnectedSignal.Task, Task.Delay(Timeout));
            Assert.Same(disconnectedSignal.Task, completed);
            Assert.Same(connectedSession, disconnectedSession);
        }

        // ---------------------------------------------------------------
        // Current is set for a OneWay dispatch too, not just request/response calls.
        // ---------------------------------------------------------------
        [Fact]
        public async Task Current_IsSet_DuringOneWayDispatch()
        {
            const string serviceId = "notify.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Notify", true));
            var observed = new TaskCompletionSource<Guid?>(TaskCreationOptions.RunContinuationsAsynchronously);

            var dispatcher = new DelegateDispatcher((request, _) =>
            {
                observed.TrySetResult(AutoServiceSessionContext.Current?.Id);
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
            await connection.Peer.SendOneWayAsync(serviceId, 1, contract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty);

            var completed = await Task.WhenAny(observed.Task, Task.Delay(Timeout));
            Assert.Same(observed.Task, completed);

            var observedSessionId = await observed.Task;
            Assert.NotNull(observedSessionId);
            Assert.Equal(host.GetAllSessions().Single().Id, observedSessionId);
        }

        // ---------------------------------------------------------------
        // The authenticator factory is invoked exactly once per accepted connection -- not once per
        // request -- and each connection gets its own count, proving the factory isn't re-run on every
        // dispatch through this session.
        // ---------------------------------------------------------------
        [Fact]
        public async Task AuthenticatorFactory_IsInvokedExactlyOncePerConnection()
        {
            const string serviceId = "echo.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Echo", false));
            var factoryCalls = 0;

            var dispatcher = new DelegateDispatcher((request, _) =>
                new ValueTask<AutoServiceResponse>(AutoServiceResponse.Success(ReadOnlyMemory<byte>.Empty)));

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithService<object>(contract, dispatcher)
                .WithAuthenticator((IAutoServicePeerSession _) =>
                {
                    Interlocked.Increment(ref factoryCalls);
                    return AutoServiceNoAuthAuthenticatorAdapter.Instance;
                })
                .StartAsync();

            using var connectionA = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();
            using var connectionB = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            await connectionA.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);
            await connectionB.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

            for (var i = 0; i < 3; i++)
            {
                Assert.True((await connectionA.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty)).IsSuccess);
                Assert.True((await connectionB.Peer.InvokeAsync(serviceId, 1, contract.Fingerprint, 1, ReadOnlyMemory<byte>.Empty)).IsSuccess);
            }

            Assert.Equal(2, factoryCalls);
        }

        // ---------------------------------------------------------------
        // The client builder's session-aware WithAuthenticator overload receives the client-side
        // session (not the server's), and its factory is invoked exactly once for the connection.
        // ---------------------------------------------------------------
        [Fact]
        public async Task ClientBuilder_SessionAwareAuthenticatorOverload_ReceivesClientSession_CalledOnce()
        {
            IAutoServicePeerSession factorySession = null;
            var factoryCalls = 0;
            var port = TestSupport.GetFreeTcpPort();

            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithAuthenticator((IAutoServicePeerSession session) =>
                {
                    Interlocked.Increment(ref factoryCalls);
                    factorySession = session;
                    return AutoServiceNoAuthAuthenticatorAdapter.Instance;
                })
                .ConnectAsync();

            await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);

            Assert.Equal(1, factoryCalls);
            Assert.NotNull(factorySession);
            Assert.Same(connection.Peer, factorySession);
        }

        private sealed class CredentialMappingAuthenticator : IAutoServiceStrictAuthenticator
        {
            private readonly IAutoServicePeerSession _session;
            private readonly ConcurrentDictionary<string, Guid> _credentialToSessionId;

            internal CredentialMappingAuthenticator(IAutoServicePeerSession session, ConcurrentDictionary<string, Guid> credentialToSessionId)
            {
                _session = session;
                _credentialToSessionId = credentialToSessionId;
            }

            public ValueTask<AutoServiceAuthenticationOutcome> AuthenticateAsync(ReadOnlyMemory<byte> credential, CancellationToken cancellationToken = default)
            {
                var credentialText = Encoding.UTF8.GetString(credential.Span);
                _credentialToSessionId[credentialText] = _session.Id;
                return new ValueTask<AutoServiceAuthenticationOutcome>(AutoServiceAuthenticationOutcome.Accepted(ReadOnlyMemory<byte>.Empty, credentialText));
            }
        }

        private sealed class CapturingAuthenticator : IAutoServiceStrictAuthenticator
        {
            private readonly byte[] _expectedCredential;
            private readonly Action _onAuthenticate;

            internal CapturingAuthenticator(byte[] expectedCredential, Action onAuthenticate)
            {
                _expectedCredential = expectedCredential;
                _onAuthenticate = onAuthenticate;
            }

            public ValueTask<AutoServiceAuthenticationOutcome> AuthenticateAsync(ReadOnlyMemory<byte> credential, CancellationToken cancellationToken = default)
            {
                _onAuthenticate();

                if (credential.Span.SequenceEqual(_expectedCredential))
                    return new ValueTask<AutoServiceAuthenticationOutcome>(AutoServiceAuthenticationOutcome.Accepted(ReadOnlyMemory<byte>.Empty, "ok"));

                return new ValueTask<AutoServiceAuthenticationOutcome>(AutoServiceAuthenticationOutcome.Refused("bad credential"));
            }
        }

        private sealed class AutoServiceNoAuthAuthenticatorAdapter : IAutoServiceStrictAuthenticator
        {
            internal static readonly AutoServiceNoAuthAuthenticatorAdapter Instance = new();

            public ValueTask<AutoServiceAuthenticationOutcome> AuthenticateAsync(ReadOnlyMemory<byte> credential, CancellationToken cancellationToken = default)
                => new(AutoServiceAuthenticationOutcome.Accepted(ReadOnlyMemory<byte>.Empty, null));
        }
    }
}
