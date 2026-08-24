using System;
using System.Net;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NetX.AutoServiceGenerator.Definitions;

namespace NetX.AutoService.Internal
{
    /// <summary>
    /// One AutoService peer bound to one NetX <see cref="INetXConnection"/> (either a server-side
    /// <see cref="INetXSession"/> or the client-side <see cref="INetXClient"/> connection -- both
    /// implement <see cref="INetXConnection"/>, which is all this class depends on).
    ///
    /// Outbound calls (<see cref="InvokeAsync(AutoServiceRequest, CancellationToken)"/> /
    /// <see cref="SendOneWayAsync(AutoServiceRequest, CancellationToken)"/>) are encoded into an
    /// <see cref="AutoServiceFrame"/> and ride NetX's own request/reply correlation
    /// (<see cref="INetXConnection.RequestAsync(ReadOnlyMemory{byte}, CancellationToken)"/> /
    /// <see cref="INetXConnection.SendAsync"/>) -- the AutoService frame's own correlation id is a
    /// constant placeholder, since NetX already guarantees the reply bytes handed back are the reply to
    /// exactly this request.
    ///
    /// Inbound frames (<see cref="RouteInboundAsync"/>) are decoded and routed through this peer's own
    /// <see cref="AutoServiceAuthenticatingDispatcher"/>, then replied to via NetX's
    /// <see cref="INetXConnection.ReplyAsync"/> using the *NetX* correlation id carried by the inbound
    /// <see cref="NetXMessage"/> (not the AutoService frame's).
    /// </summary>
    internal sealed class AutoServicePeerSession : IAutoServicePeerSession, IAutoServiceTransport, IAutoServicePeer
    {
        // AutoServiceFrame requires a non-zero correlation id on Request/Response frames, but nothing
        // in this stack ever reads it back out to correlate anything -- NetX's own frame correlation
        // id is what every reply is actually matched against. A constant keeps encode/decode honest
        // without pretending to track a second, unused correlation space.
        private const ulong FrameCorrelationId = 1;

        private readonly INetXConnection _connection;
        private readonly AutoServiceRouter _router;
        private readonly AutoServiceAuthenticatingDispatcher _dispatcher;
        private readonly AutoServiceCallContextFactory _outgoingContextFactory = new();
        private readonly int _maxFrameBytes;

        // NetX's read loop (NetXConnection.ReadPipeAsync) awaits OnReceivedMessageAsync before it reads
        // the next frame. The bounded queue therefore admits and decodes frames in arrival order, then
        // returns immediately to the read loop. Operation 0/auth is the one ordering barrier: the worker
        // dispatches it inline and awaits it before starting the next frame. Non-auth dispatches acquire
        // one of the bounded slots and continue on detached tasks, so nested calls can re-enter this
        // peer while an outer handler is awaiting a reply. Completion order among non-auth calls is not
        // ordered. A queue overflow disconnects instead of waiting for the read loop: the reply that
        // could unblock a handler arrives on that same loop, so waiting here would recreate the deadlock.
        // Reply frames never reach this queue -- NetX matches those against pending RequestAsync
        // completions before a message is handed to a processor. A nesting chain deeper than the slot
        // count can therefore wait until a timeout or queue overflow; no immediate disconnect is
        // promised for that case. Reentrant dispatch is guaranteed only after authentication: op-0 is
        // intentionally an inline barrier, so an authenticator must not synchronously call back into
        // this same peer and await another inbound operation.
        private readonly Channel<PendingInbound> _inboundQueue;
        private readonly SemaphoreSlim _dispatchSlots;
        private readonly CancellationTokenSource _lifetimeCts;
        private int _inboundCompleted;

        private readonly struct PendingInbound
        {
            internal PendingInbound(NetXMessage message, CancellationToken cancellationToken)
            {
                Message = message;
                CancellationToken = cancellationToken;
            }

            internal NetXMessage Message { get; }
            internal CancellationToken CancellationToken { get; }
        }

        internal AutoServicePeerSession(
            INetXConnection connection,
            Guid id,
            EndPoint remoteEndPoint,
            AutoServiceRouter router,
            Func<IAutoServicePeerSession, IAutoServiceStrictAuthenticator> authenticatorFactory,
            int maxFrameBytes,
            int maxConcurrentDispatches,
            int maxPendingInbound)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            Id = id;
            RemoteEndPoint = remoteEndPoint;
            _router = router ?? throw new ArgumentNullException(nameof(router));
            _maxFrameBytes = maxFrameBytes;
            if (maxConcurrentDispatches <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxConcurrentDispatches), maxConcurrentDispatches, "The dispatch concurrency must be positive.");
            if (maxPendingInbound <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxPendingInbound), maxPendingInbound, "The pending inbound capacity must be positive.");

            _inboundQueue = Channel.CreateBounded<PendingInbound>(new BoundedChannelOptions(maxPendingInbound)
            {
                // Wait mode makes TryWrite report a full queue instead of silently dropping an item.
                // This peer never calls WriteAsync/WaitToWriteAsync, so the NetX read loop never waits.
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true,
            });
            _dispatchSlots = new SemaphoreSlim(maxConcurrentDispatches, maxConcurrentDispatches);
            _lifetimeCts = new CancellationTokenSource();
            // The factory is invoked with `this` constructed enough for a session-aware authenticator to
            // address this peer by Id/RemoteEndPoint/Transport (e.g. to correlate it against
            // connection-level state) -- but before `_dispatcher` exists and before authentication has
            // happened. The factory must not call back into dispatch (IsAuthenticated/Principal read
            // `_dispatcher`, which is still null here; see the null-safe getters below) or invoke this
            // peer's own outbound/inbound methods.
            var authenticator = authenticatorFactory?.Invoke(this) ?? AutoServiceNoAuthAuthenticator.Instance;
            _dispatcher = new AutoServiceAuthenticatingDispatcher(router, authenticator);
            // The worker is deliberately detached: NetX owns connection lifetime, while this peer
            // drains/disposes its queue and detached dispatches independently during teardown.
            _ = Task.Run(ProcessInboundQueueAsync);
        }

        public Guid Id { get; }
        public EndPoint RemoteEndPoint { get; }
        public bool IsConnected => _connection.IsConnected;
        // Null-safe: a session-aware authenticator factory (see ctor) can read these before
        // `_dispatcher` is assigned.
        public bool IsAuthenticated => _dispatcher?.IsAuthenticated ?? false;
        public object Principal => _dispatcher?.Principal;
        public IAutoServiceTransport Transport => this;

        public async ValueTask<AutoServiceResponse> InvokeAsync(AutoServiceRequest request, CancellationToken cancellationToken = default)
        {
            if (request == null)
                throw new ArgumentNullException(nameof(request));

            byte[] encoded;
            try
            {
                var frame = AutoServiceFrame.Request(
                    FrameCorrelationId,
                    request.LogicalCallId,
                    request.LogicalCallSequence,
                    request.ServiceId,
                    request.SchemaVersion,
                    request.ContractFingerprint,
                    request.OperationId,
                    request.Payload);
                encoded = AutoServiceFrameCodec.Encode(frame, _maxFrameBytes);
            }
            catch (Exception ex)
            {
                return AutoServiceResponse.Failure(AutoServiceErrorCode.InvalidRequest, ex.Message);
            }

            using var reply = await _connection.RequestAsync(encoded, cancellationToken).ConfigureAwait(false);

            AutoServiceFrame responseFrame;
            try
            {
                responseFrame = AutoServiceFrameCodec.Decode(reply.Buffer.Span, _maxFrameBytes);
            }
            catch (Exception ex)
            {
                return AutoServiceResponse.Failure(AutoServiceErrorCode.Internal, "Malformed AutoService response: " + ex.Message);
            }

            return responseFrame.Status == AutoServiceFrameStatus.Ok
                ? AutoServiceResponse.Success(responseFrame.Payload)
                : AutoServiceResponse.Failure(responseFrame.ErrorCode, responseFrame.ErrorMessage, responseFrame.Retryable);
        }

        public ValueTask<AutoServiceResponse> InvokeAsync(
            string serviceId,
            int schemaVersion,
            string contractFingerprint,
            int operationId,
            ReadOnlyMemory<byte> payload,
            CancellationToken cancellationToken = default)
        {
            var request = BuildOutgoingRequest(serviceId, schemaVersion, contractFingerprint, operationId, payload);
            return InvokeAsync(request, cancellationToken);
        }

        public async ValueTask SendOneWayAsync(AutoServiceRequest request, CancellationToken cancellationToken = default)
        {
            if (request == null)
                throw new ArgumentNullException(nameof(request));

            var frame = AutoServiceFrame.OneWay(
                request.LogicalCallId,
                request.LogicalCallSequence,
                request.ServiceId,
                request.SchemaVersion,
                request.ContractFingerprint,
                request.OperationId,
                request.Payload);
            var encoded = AutoServiceFrameCodec.Encode(frame, _maxFrameBytes);
            await _connection.SendAsync(encoded, cancellationToken).ConfigureAwait(false);
        }

        public ValueTask SendOneWayAsync(
            string serviceId,
            int schemaVersion,
            string contractFingerprint,
            int operationId,
            ReadOnlyMemory<byte> payload,
            CancellationToken cancellationToken = default)
        {
            var request = BuildOutgoingRequest(serviceId, schemaVersion, contractFingerprint, operationId, payload);
            return SendOneWayAsync(request, cancellationToken);
        }

        public async Task<AutoServiceResponse> AuthenticateAsync(ReadOnlyMemory<byte> credential, CancellationToken cancellationToken = default)
        {
            var response = await InvokeAsync(
                AutoServiceAuthContract.ServiceId,
                AutoServiceAuthContract.SchemaVersion,
                AutoServiceAuthContract.ContractFingerprint,
                AutoServiceAuthentication.OperationId,
                credential,
                cancellationToken).ConfigureAwait(false);
            return response;
        }

        public void Disconnect() => _connection.Disconnect();

        private AutoServiceRequest BuildOutgoingRequest(string serviceId, int schemaVersion, string contractFingerprint, int operationId, ReadOnlyMemory<byte> payload)
        {
            var context = _outgoingContextFactory.Create(serviceId, schemaVersion, operationId, contractFingerprint, payload);
            return new AutoServiceRequest(context, payload);
        }

        /// <summary>
        /// Hands <paramref name="message"/> off to this peer's bounded inbound queue and returns
        /// immediately -- called synchronously from the owning processor's OnReceivedMessageAsync so the
        /// NetX read loop can move straight on to the next frame. Takes ownership of
        /// <paramref name="message"/>: it is either queued (and later disposed after decode) or disposed
        /// here. A full active queue is a protocol-level overflow and disconnects the peer; a completed
        /// queue only disposes the late message because teardown is already in progress.
        /// </summary>
        internal void EnqueueInbound(NetXMessage message, CancellationToken cancellationToken)
        {
            if (Volatile.Read(ref _inboundCompleted) != 0
                || !_inboundQueue.Writer.TryWrite(new PendingInbound(message, cancellationToken)))
            {
                message.Dispose();

                // TryWrite can fail because the bounded queue is full or because CompleteInbound raced
                // with this enqueue. Only the former is an overflow requiring a new disconnect.
                if (Volatile.Read(ref _inboundCompleted) == 0)
                    Disconnect();
            }
        }

        /// <summary>
        /// Stops accepting further inbound frames, wakes any worker waiting for a dispatch slot, and
        /// disposes whatever remains queued. Idempotent; called by the owning processor when its NetX
        /// session/connection disconnects.
        /// </summary>
        internal void CompleteInbound()
        {
            if (Interlocked.Exchange(ref _inboundCompleted, 1) != 0)
                return;

            _lifetimeCts.Cancel();
            _inboundQueue.Writer.TryComplete();
        }

        private async Task ProcessInboundQueueAsync()
        {
            try
            {
                await foreach (var pending in _inboundQueue.Reader.ReadAllAsync().ConfigureAwait(false))
                {
                    await RouteInboundAsync(pending.Message, pending.CancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (_lifetimeCts.IsCancellationRequested)
            {
                // Completion cancels the lifetime to release a slot wait; the finally block still owns
                // disposal of every item that was left behind in the queue.
            }
            catch (Exception)
            {
                // A worker failure means this peer can no longer provide a reliable reply/protocol
                // result. Complete first so late frames are disposed even before NetX reports the
                // disconnect back to the processor; then tear down the physical connection.
                CompleteInbound();
                try { Disconnect(); } catch { }
            }
            finally
            {
                while (_inboundQueue.Reader.TryRead(out var pending))
                    pending.Message.Dispose();
            }
        }

        /// <summary>
        /// Decodes and validates one inbound message on the FIFO worker. The pooled NetX message is
        /// disposed immediately after decode because the codec owns a heap copy of the payload. Auth
        /// (operation 0) is dispatched inline as the ordering barrier. Before authentication completes,
        /// non-auth calls also remain inline so a frame that arrived before auth cannot be scheduled after
        /// a later auth frame and accidentally observe authenticated state. Other calls acquire a bounded
        /// slot and are detached so nested duplex dispatch can re-enter this peer.
        /// </summary>
        private async Task RouteInboundAsync(NetXMessage message, CancellationToken cancellationToken)
        {
            // Capture these before disposing the pooled NetX message. The AutoService-level decoder
            // copies payload bytes into the returned frame, while the NetX correlation id is metadata
            // owned by the message itself.
            var correlationId = message.CorrelationId;
            var expectsReply = !message.IsPush;

            AutoServiceFrame frame;
            try
            {
                frame = AutoServiceFrameCodec.Decode(message.Buffer.Span, _maxFrameBytes);
            }
            catch (Exception ex)
            {
                message.Dispose();
                // A malformed request can still receive a structured protocol error because NetX's
                // correlation id was parsed outside the AutoService payload. A push has no reply path,
                // so it is a protocol violation and must disconnect.
                if (expectsReply)
                    await ReplyWithProtocolErrorAsync(correlationId, DescribeDecodeFailure(ex), cancellationToken).ConfigureAwait(false);
                else
                    _connection.Disconnect();
                return;
            }

            // Decode copied the payload; return the pooled buffer before any dispatch can block.
            message.Dispose();

            // A response frame should never reach here -- NetX already matches replies against pending
            // RequestAsync completions before a message is handed to a processor. Defensive no-op.
            if (frame.Kind == AutoServiceFrameKind.Response)
                return;

            // AutoServiceRequest/AutoServiceCallContext do not carry which physical frame kind (Request
            // vs OneWay) arrived, so validate the mismatch at this binding where both facts are known.
            if (frame.OperationId != AutoServiceAuthentication.OperationId
                && _router.TryGetOperationDescriptor(frame.ServiceId, frame.OperationId, out var operation)
                && operation.OneWay != (frame.Kind == AutoServiceFrameKind.OneWay))
            {
                var mismatchMessage = operation.OneWay
                    ? $"Operation '{operation.Name}' ({frame.ServiceId}/{frame.OperationId}) is declared OneWay and cannot be invoked as a request expecting a reply."
                    : $"Operation '{operation.Name}' ({frame.ServiceId}/{frame.OperationId}) is not declared OneWay and requires a request expecting a reply.";

                if (expectsReply)
                    await ReplyWithProtocolErrorAsync(correlationId, mismatchMessage, cancellationToken).ConfigureAwait(false);
                else
                    _connection.Disconnect();
                return;
            }

            // Operation zero is the authentication barrier. Keep pre-auth calls inline as well: this
            // preserves the FIFO security decision when a non-auth frame arrived before a later auth
            // frame but the latter has already reached the worker. Once authenticated, non-auth calls
            // may proceed concurrently.
            if (frame.OperationId == AutoServiceAuthentication.OperationId || !_dispatcher.IsAuthenticated)
            {
                await DispatchAndReplyAsync(frame, expectsReply, correlationId, cancellationToken).ConfigureAwait(false);
                return;
            }

            await _dispatchSlots.WaitAsync(_lifetimeCts.Token).ConfigureAwait(false);
            // This task deliberately owns its slot until dispatch and reply handling complete. Catch all
            // failures so the detached Task.Run never faults as an unobserved task.
            _ = Task.Run(async () =>
            {
                try
                {
                    await DispatchAndReplyAsync(frame, expectsReply, correlationId, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    try { Disconnect(); } catch { }
                }
                finally
                {
                    try { _dispatchSlots.Release(); } catch { }
                }
            });
        }

        /// <summary>
        /// Dispatches one decoded frame and, when the physical frame expects a reply, encodes and sends
        /// that reply using NetX's correlation id. Dispatcher/service failures retain the previous
        /// structured internal-error response semantics; transport/protocol failures escape to the
        /// owning inline worker or detached wrapper, which disconnects the peer.
        /// </summary>
        private async Task DispatchAndReplyAsync(
            AutoServiceFrame frame,
            bool expectsReply,
            ulong correlationId,
            CancellationToken cancellationToken)
        {
            AutoServiceResponse response;
            try
            {
                var context = AutoServiceCallContext.Receive(
                    frame.LogicalCallId,
                    frame.LogicalCallSequence,
                    frame.ServiceId,
                    frame.SchemaVersion,
                    frame.OperationId,
                    frame.ContractFingerprint,
                    frame.Payload);
                var request = new AutoServiceRequest(context, frame.Payload);
                using (AutoServiceSessionContext.Begin(this))
                {
                    response = await _dispatcher.DispatchAsync(request, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                response = AutoServiceResponse.Failure(AutoServiceErrorCode.Internal, ex.Message);
            }

            if (!expectsReply)
                return;

            byte[] encoded;
            try
            {
                var responseFrame = AutoServiceFrame.Response(
                    FrameCorrelationId,
                    frame.LogicalCallId,
                    frame.LogicalCallSequence,
                    frame.ServiceId,
                    frame.SchemaVersion,
                    frame.ContractFingerprint,
                    frame.OperationId,
                    response);
                encoded = AutoServiceFrameCodec.Encode(responseFrame, _maxFrameBytes);
            }
            catch (Exception ex)
            {
                var failureFrame = AutoServiceFrame.Response(
                    FrameCorrelationId,
                    frame.LogicalCallId,
                    frame.LogicalCallSequence,
                    frame.ServiceId,
                    frame.SchemaVersion,
                    frame.ContractFingerprint,
                    frame.OperationId,
                    AutoServiceResponse.Failure(AutoServiceErrorCode.Internal, "Failed to encode response: " + ex.Message));
                encoded = AutoServiceFrameCodec.Encode(failureFrame, _maxFrameBytes);
            }

            await _connection.ReplyAsync(correlationId, encoded, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Replies through NetX's own correlation id (bypassing the undecodable/mismatched AutoService
        /// frame entirely) with a structured <see cref="AutoServiceErrorCode.InvalidRequest"/> response
        /// built on the <see cref="AutoServiceProtocolErrorContract"/> sentinel identity.
        /// </summary>
        private async ValueTask ReplyWithProtocolErrorAsync(ulong correlationId, string message, CancellationToken cancellationToken)
        {
            byte[] encoded;
            try
            {
                var frame = AutoServiceFrame.Response(
                    FrameCorrelationId,
                    AutoServiceProtocolErrorContract.LogicalCallId,
                    AutoServiceProtocolErrorContract.LogicalCallSequence,
                    AutoServiceProtocolErrorContract.ServiceId,
                    AutoServiceProtocolErrorContract.SchemaVersion,
                    AutoServiceProtocolErrorContract.ContractFingerprint,
                    AutoServiceProtocolErrorContract.OperationId,
                    AutoServiceResponse.Failure(AutoServiceErrorCode.InvalidRequest, message));
                encoded = AutoServiceFrameCodec.Encode(frame, _maxFrameBytes);
            }
            catch
            {
                // Even the minimal sentinel error frame does not fit within maxFrameBytes -- nothing
                // safe to reply with. Disconnect explicitly rather than leaving the caller to time out.
                _connection.Disconnect();
                return;
            }

            await _connection.ReplyAsync(correlationId, encoded, cancellationToken).ConfigureAwait(false);
        }

        private static string DescribeDecodeFailure(Exception ex) =>
            "The AutoService request could not be decoded: " + ex.Message;
    }
}
