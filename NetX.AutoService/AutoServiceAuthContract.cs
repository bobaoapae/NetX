using System;

namespace NetX.AutoService
{
    /// <summary>
    /// Pseudo contract identity used only to shape the operation-0 authentication frame.
    /// <see cref="NetX.AutoServiceGenerator.Definitions.AutoServiceAuthenticatingDispatcher"/> never
    /// consults <see cref="NetX.AutoServiceGenerator.Definitions.AutoServiceRouter"/> for operation 0
    /// (it intercepts it before the inner dispatcher is ever reached), so this identity does not need
    /// to match any registered service -- it exists purely so the frame carries a well-formed
    /// service id / schema version / fingerprint triple.
    /// </summary>
    internal static class AutoServiceAuthContract
    {
        internal const string ServiceId = "netx.autoservice.auth";
        internal const int SchemaVersion = 1;

        // A 64-character lowercase-hex placeholder satisfying AutoServiceCallContext's fingerprint
        // shape check. Its value is never compared against anything for operation 0.
        internal static readonly string ContractFingerprint = new string('0', 64);
    }

    /// <summary>
    /// Pseudo contract identity used to shape a protocol-error response -- one built for an inbound
    /// frame this peer could not decode (e.g. its AutoService-level payload exceeded the configured
    /// <c>MaxFrameBytes</c>) or an operation whose Request/OneWay kind did not match its descriptor.
    /// </summary>
    /// <remarks>
    /// The peer that receives this response only ever inspects its <c>Status</c>/<c>Error</c>
    /// (see <see cref="Internal.AutoServicePeerSession.InvokeAsync(NetX.AutoServiceGenerator.Definitions.AutoServiceRequest, System.Threading.CancellationToken)"/>)
    /// -- it never checks a response frame's identity against the request it sent -- so a stable
    /// sentinel identity is sufficient; it does not need to echo anything from the (possibly
    /// undecodable) inbound frame.
    /// </remarks>
    internal static class AutoServiceProtocolErrorContract
    {
        internal const string ServiceId = "netx.autoservice.protocol-error";
        internal const int SchemaVersion = 1;
        internal const long LogicalCallSequence = 1;
        internal const int OperationId = 0;

        internal static readonly Guid LogicalCallId = new("00000000-0000-0000-0000-0000000000ee");

        // A 64-character lowercase-hex placeholder satisfying AutoServiceCallContext's fingerprint
        // shape check. Its value is never compared against anything for this sentinel identity.
        internal static readonly string ContractFingerprint = new string('0', 64);
    }
}
