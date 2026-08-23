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
    /// Inbound frames (<see cref="HandleInboundAsync"/>) are decoded and routed through this peer's own
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
        // the next frame. Two things are both true at once here:
        //  1. A dispatcher invoked from HandleInboundAsync may call back into this same peer (a reverse
        //     operation over the same connection) before it finishes handling the forward call -- that
        //     reverse call's reply can only ever arrive on this connection's read loop, so
        //     OnReceivedMessageAsync must return before HandleInboundAsync completes, or the nested
        //     round trip deadlocks.
        //  2. Frames must still be handled in the order NetX handed them to this peer -- letting each
        //     one dispatch fully concurrently (fire-and-forget per message) can run frame N and frame
        //     N+1 side by side, and two ordered calls (e.g. authenticate then a follow-up op) can
        //     execute or complete out of order.
        // This queue is the resolution: EnqueueInbound hands the message off and returns immediately
        // (satisfying #1), while a single background worker drains it one message at a time via
        // HandleInboundAsync (satisfying #2). Reply frames never reach here -- NetX matches those
        // against pending RequestAsync completions before a message is ever handed to a processor (see
        // the Response-frame no-op inside HandleInboundAsync) -- so a reverse call's reply is free to
        // land on the read loop and complete while this queue is still draining an earlier forward call.
        private readonly Channel<PendingInbound> _inboundQueue = Channel.CreateUnbounded<PendingInbound>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
        private readonly Task _inboundWorker;

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
            int maxFrameBytes)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            Id = id;
            RemoteEndPoint = remoteEndPoint;
            _router = router ?? throw new ArgumentNullException(nameof(router));
            _maxFrameBytes = maxFrameBytes;
            // The factory is invoked with `this` constructed enough for a session-aware authenticator to
            // address this peer by Id/RemoteEndPoint/Transport (e.g. to correlate it against
            // connection-level state) -- but before `_dispatcher` exists and before authentication has
            // happened. The factory must not call back into dispatch (IsAuthenticated/Principal read
            // `_dispatcher`, which is still null here; see the null-safe getters below) or invoke this
            // peer's own outbound/inbound methods.
            var authenticator = authenticatorFactory?.Invoke(this) ?? AutoServiceNoAuthAuthenticator.Instance;
            _dispatcher = new AutoServiceAuthenticatingDispatcher(router, authenticator);
            _inboundWorker = Task.Run(ProcessInboundQueueAsync);
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
        /// Hands <paramref name="message"/> off to this peer's serial inbound queue and returns
        /// immediately -- called synchronously from the owning processor's OnReceivedMessageAsync so the
        /// NetX read loop can move straight on to the next frame (see the queue's own doc comment above
        /// for why that matters). Takes ownership of <paramref name="message"/>: it is either queued (and
        /// later disposed by <see cref="HandleInboundAsync"/>) or, if the queue has already been
        /// completed (this peer disconnected), disposed here.
        /// </summary>
        internal void EnqueueInbound(NetXMessage message, CancellationToken cancellationToken)
        {
            if (!_inboundQueue.Writer.TryWrite(new PendingInbound(message, cancellationToken)))
                message.Dispose();
        }

        /// <summary>
        /// Stops accepting further inbound frames and lets the worker drain whatever is already queued.
        /// Called once by the owning processor when its NetX session/connection disconnects.
        /// </summary>
        internal void CompleteInbound() => _inboundQueue.Writer.TryComplete();

        private async Task ProcessInboundQueueAsync()
        {
            await foreach (var pending in _inboundQueue.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                try
                {
                    await HandleInboundAsync(pending.Message, pending.CancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Shutdown/cancellation of the owning connection -- nothing left to reply to.
                }
                catch (Exception)
                {
                    // HandleInboundAsync already turns dispatcher/service failures into a structured
                    // AutoServiceResponse and replies with it -- reaching here means the reply/protocol-
                    // error write itself (or NetX's own send path underneath it) failed unexpectedly.
                    // There is no reply channel left to trust, so tear the connection down rather than
                    // leave a caller hanging on a request this connection can no longer honor.
                    Disconnect();
                }
            }
        }

        /// <summary>
        /// Decodes one inbound <see cref="NetXMessage"/> as an AutoService frame, dispatches it through
        /// this peer's <see cref="AutoServiceAuthenticatingDispatcher"/>, and (for request frames) replies
        /// via NetX's own correlation id. Takes ownership of <paramref name="message"/> (always disposed).
        /// </summary>
        internal async Task HandleInboundAsync(NetXMessage message, CancellationToken cancellationToken)
        {
            using var owned = message;

            // NetX's own correlation id (not the AutoService frame's own, constant, FrameCorrelationId)
            // is what tells us whether the peer is waiting on a reply via RequestAsync (non-zero) or
            // sent a push/one-way frame via SendAsync (zero, see AutoServicePeerSession.SendOneWayAsync).
            // Crucially this is known even when the AutoService-level frame below turns out to be
            // undecodable, because NetX parses its own envelope before this processor is ever reached.
            var expectsReply = !owned.IsPush;

            AutoServiceFrame frame;
            try
            {
                frame = AutoServiceFrameCodec.Decode(owned.Buffer.Span, _maxFrameBytes);
            }
            catch (Exception ex)
            {
                // Bug fix: this used to be a silent drop for every decode failure, including a payload
                // that merely exceeded the configured AutoService-level MaxFrameBytes (NetX's own,
                // larger, MaxFrameBytes + AutoServiceNetXFrameLimits.OverheadMargin guard already let
                // the frame through -- see AutoServiceNetXServerBuilder.StartAsync). A caller awaiting
                // InvokeAsync on the other end would then hang until its own duplex timeout fired,
                // observing a silent timeout instead of a structured error for what is, from the wire's
                // perspective, a clean and legible rejection. If NetX's own correlation id tells us a
                // reply is expected, we always have enough to answer structurally regardless of what
                // failed to decode inside the AutoService frame itself. Otherwise (a one-way/push frame,
                // which has no reply channel to answer on) there is no way to signal the failure back to
                // the sender other than tearing down the connection as a protocol violation -- silently
                // dropping it would leave the sender believing delivery succeeded.
                if (expectsReply)
                    await ReplyWithProtocolErrorAsync(owned.CorrelationId, DescribeDecodeFailure(ex), cancellationToken).ConfigureAwait(false);
                else
                    _connection.Disconnect();
                return;
            }

            // A response frame should never reach here -- NetX already matches replies against pending
            // RequestAsync completions before a message is ever handed to a processor. Defensive no-op.
            if (frame.Kind == AutoServiceFrameKind.Response)
                return;

            // Gap (flagged for the ASG generator, see AutoServiceRouter.TryGetOperationDescriptor):
            // AutoServiceRequest/AutoServiceCallContext do not carry which physical frame kind (Request
            // vs OneWay) an inbound call arrived as, so AutoServiceRouter.DispatchAsync itself cannot
            // validate it against AutoServiceOperationDescriptor.OneWay. This binding knows the kind
            // (frame.Kind) and can look the descriptor up, so it validates here -- the one place in this
            // stack both facts are available -- instead of letting a Request sent for a declared OneWay
            // operation (or vice versa) dispatch ambiguously (e.g. a OneWay caller silently never
            // learning a "reply" it will never receive was actually swallowed, or a Request caller
            // hanging because a OneWay-only handler never produces one).
            if (frame.OperationId != AutoServiceAuthentication.OperationId
                && _router.TryGetOperationDescriptor(frame.ServiceId, frame.OperationId, out var operation)
                && operation.OneWay != (frame.Kind == AutoServiceFrameKind.OneWay))
            {
                var mismatchMessage = operation.OneWay
                    ? $"Operation '{operation.Name}' ({frame.ServiceId}/{frame.OperationId}) is declared OneWay and cannot be invoked as a request expecting a reply."
                    : $"Operation '{operation.Name}' ({frame.ServiceId}/{frame.OperationId}) is not declared OneWay and requires a request expecting a reply.";

                if (expectsReply)
                    await ReplyWithProtocolErrorAsync(owned.CorrelationId, mismatchMessage, cancellationToken).ConfigureAwait(false);
                else
                    // A OneWay frame invoking a non-OneWay operation has no reply channel to explain
                    // itself on -- same rationale as the decode-failure branch above.
                    _connection.Disconnect();
                return;
            }

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

            if (frame.Kind == AutoServiceFrameKind.OneWay)
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

            await _connection.ReplyAsync(owned.CorrelationId, encoded, cancellationToken).ConfigureAwait(false);
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
