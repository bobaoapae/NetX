using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetX.AutoServiceGenerator.Definitions;
using Xunit;

namespace NetX.AutoService.Tests
{
    /// <summary>Regression coverage for bounded inbound dispatch and clean peer teardown.</summary>
    public class InboundPipelineTests
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

        [Fact]
        public async Task InboundOverflow_DisconnectsNoHang_AndReleasesBlockedDispatch()
        {
            const string serviceId = "inbound.overflow.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Block", false));
            var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var clientDisconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var dispatcher = new DelegateDispatcher(async (request, _) =>
            {
                handlerEntered.TrySetResult();
                await releaseHandler.Task;
                return AutoServiceResponse.Success(request.Payload);
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithMaxConcurrentDispatches(1)
                .WithMaxPendingInbound(1)
                .WithService<object>(contract, dispatcher)
                .OnSessionDisconnected((_, _) =>
                {
                    disconnected.TrySetResult();
                    return ValueTask.CompletedTask;
                })
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .OnDisconnected(_ =>
                {
                    clientDisconnected.TrySetResult();
                    return ValueTask.CompletedTask;
                })
                .ConnectAsync();

            var authenticated = await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);
            Assert.True(authenticated.IsSuccess);

            var first = connection.Peer.InvokeAsync(
                serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("block")).AsTask();
            await handlerEntered.Task.WaitAsync(Timeout);

            // The first request occupies the only dispatch slot. A second item fills the one-item
            // inbound queue; the remaining requests must fail closed by disconnecting the peer rather
            // than blocking NetX's read loop waiting for a slot.
            var additional = Enumerable.Range(1, 6)
                .Select(index => connection.Peer.InvokeAsync(
                    serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("queued-" + index)).AsTask())
                .ToArray();
            var requests = new List<Task<AutoServiceResponse>> { first };
            requests.AddRange(additional);

            var disconnectedResult = await Task.WhenAny(disconnected.Task, Task.Delay(Timeout));
            var didDisconnect = ReferenceEquals(disconnectedResult, disconnected.Task);

            releaseHandler.TrySetResult();
            try { await Task.WhenAll(requests); } catch { }

            var clientDisconnectedResult = await Task.WhenAny(clientDisconnected.Task, Task.Delay(Timeout));

            Assert.True(didDisconnect, "Inbound overflow did not disconnect the peer before the timeout.");
            Assert.Same(clientDisconnected.Task, clientDisconnectedResult);
            Assert.False(connection.Peer.IsConnected);
        }

        [Fact]
        public async Task Disconnect_WithBlockedSlotAndQueuedFrames_CompletesWithoutWorkerException()
        {
            const string serviceId = "inbound.teardown.v1";
            var contract = TestSupport.BuildContract(serviceId, 1, (1, "Block", false));
            var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var handled = 0;

            var dispatcher = new DelegateDispatcher(async (request, _) =>
            {
                Interlocked.Increment(ref handled);
                handlerEntered.TrySetResult();
                await releaseHandler.Task;
                return AutoServiceResponse.Success(request.Payload);
            });

            var port = TestSupport.GetFreeTcpPort();
            using var host = await AutoServiceNetXServer.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .WithMaxConcurrentDispatches(1)
                .WithMaxPendingInbound(4)
                .WithService<object>(contract, dispatcher)
                .OnSessionDisconnected((_, _) =>
                {
                    disconnected.TrySetResult();
                    return ValueTask.CompletedTask;
                })
                .StartAsync();

            using var connection = await AutoServiceNetXClient.Create()
                .WithEndPoint(IPAddress.Loopback.ToString(), (ushort)port)
                .ConnectAsync();

            var authenticated = await connection.AuthenticateAsync(ReadOnlyMemory<byte>.Empty);
            Assert.True(authenticated.IsSuccess);

            var serverSession = host.GetAllSessions().Single();
            var first = connection.Peer.InvokeAsync(
                serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("block")).AsTask();
            await handlerEntered.Task.WaitAsync(Timeout);

            var queued = Enumerable.Range(1, 3)
                .Select(index => connection.Peer.InvokeAsync(
                    serviceId, 1, contract.Fingerprint, 1, Encoding.UTF8.GetBytes("queued-" + index)).AsTask())
                .ToArray();
            var requests = new List<Task<AutoServiceResponse>> { first };
            requests.AddRange(queued);

            // Let the already-started NetX read loop enqueue the requests while the only dispatch slot
            // remains occupied. This is a scheduling yield, not the acceptance timeout.
            await Task.Delay(100);
            serverSession.Disconnect();

            var disconnectedResult = await Task.WhenAny(disconnected.Task, Task.Delay(Timeout));
            var allDisconnected = ReferenceEquals(disconnectedResult, disconnected.Task);

            releaseHandler.TrySetResult();
            var all = Task.WhenAll(requests);
            var allSettledResult = await Task.WhenAny(all, Task.Delay(Timeout));
            var allSettled = ReferenceEquals(allSettledResult, all);
            try { await all; } catch { }

            Assert.True(allDisconnected, "Explicit disconnect did not complete before the timeout.");
            Assert.True(allSettled, "Queued requests were left hanging after peer teardown.");
            Assert.False(serverSession.IsConnected);
            Assert.Equal(1, Volatile.Read(ref handled));
        }
    }
}
