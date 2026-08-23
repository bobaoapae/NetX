using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using NetX.AutoServiceGenerator.Definitions;

namespace NetX.AutoService
{
    /// <summary>
    /// Abstraction over one logical AutoService peer riding on top of one NetX <see cref="INetXConnection"/>.
    /// The same shape works both server-side (one instance per accepted <see cref="INetXSession"/>) and
    /// client-side (one instance for the single outbound <see cref="INetXClient"/> connection), which is
    /// what makes duplex/reverse calls possible: whichever side initiates a call, it goes through the
    /// same <see cref="InvokeAsync(AutoServiceRequest, CancellationToken)"/> / <see cref="SendOneWayAsync(AutoServiceRequest, CancellationToken)"/> seam.
    /// </summary>
    public interface IAutoServicePeerSession
    {
        /// <summary>Stable identifier for this peer/session (a fresh <see cref="Guid"/> client-side, the NetX session id server-side).</summary>
        Guid Id { get; }

        /// <summary>Remote endpoint, when known (server-side sessions always know it; a client's outbound connection may not expose one).</summary>
        EndPoint RemoteEndPoint { get; }

        bool IsConnected { get; }

        /// <summary>Whether this peer completed the operation-0 authentication handshake as the *inbound* (dispatching) side.</summary>
        bool IsAuthenticated { get; }

        /// <summary>Application-owned identity established by the configured <see cref="IAutoServiceStrictAuthenticator"/>, once authenticated.</summary>
        object Principal { get; }

        /// <summary>The transport-neutral seam a generated/hand-written proxy would target to reach this peer.</summary>
        IAutoServiceTransport Transport { get; }

        /// <summary>Sends a duplex AutoService request and awaits its response.</summary>
        ValueTask<AutoServiceResponse> InvokeAsync(AutoServiceRequest request, CancellationToken cancellationToken = default);

        /// <summary>Convenience overload that builds the <see cref="AutoServiceRequest"/> (and its call context) from raw identity/payload.</summary>
        ValueTask<AutoServiceResponse> InvokeAsync(
            string serviceId,
            int schemaVersion,
            string contractFingerprint,
            int operationId,
            ReadOnlyMemory<byte> payload,
            CancellationToken cancellationToken = default);

        /// <summary>Sends a fire-and-forget AutoService call. No response frame is expected or awaited.</summary>
        ValueTask SendOneWayAsync(AutoServiceRequest request, CancellationToken cancellationToken = default);

        /// <summary>Convenience overload that builds the <see cref="AutoServiceRequest"/> (and its call context) from raw identity/payload.</summary>
        ValueTask SendOneWayAsync(
            string serviceId,
            int schemaVersion,
            string contractFingerprint,
            int operationId,
            ReadOnlyMemory<byte> payload,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Performs the operation-0 handshake against this peer's inbound dispatcher (i.e. asks the far
        /// side to authenticate this connection). On success, the *far side's* session (not this one)
        /// observes <see cref="IsAuthenticated"/>/<see cref="Principal"/> flip.
        /// </summary>
        Task<AutoServiceResponse> AuthenticateAsync(ReadOnlyMemory<byte> credential, CancellationToken cancellationToken = default);

        void Disconnect();
    }
}
