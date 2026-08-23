using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetX.AutoServiceGenerator.Definitions;

namespace NetX.AutoService.Tests
{
    internal static class TestSupport
    {
        private const string TestContractFingerprint = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

        internal static int GetFreeTcpPort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try
            {
                return ((IPEndPoint)listener.LocalEndpoint).Port;
            }
            finally
            {
                listener.Stop();
            }
        }

        internal static AutoServiceContractDescriptor BuildContract(string serviceId, int schemaVersion, params (int Id, string Name, bool OneWay)[] operations)
        {
            var descriptors = operations
                .Select(op => new AutoServiceOperationDescriptor(op.Id, op.Name, "bytes", "bytes", AutoServiceOperationPolicy.EphemeralReadControl, op.OneWay, 0))
                .ToArray();
            return new AutoServiceContractDescriptor(serviceId, schemaVersion, TestContractFingerprint, descriptors);
        }
    }

    internal sealed class DelegateDispatcher : IAutoServiceDispatcher
    {
        private readonly Func<AutoServiceRequest, CancellationToken, ValueTask<AutoServiceResponse>> _handler;

        internal DelegateDispatcher(Func<AutoServiceRequest, CancellationToken, ValueTask<AutoServiceResponse>> handler)
        {
            _handler = handler;
        }

        public ValueTask<AutoServiceResponse> DispatchAsync(AutoServiceRequest request, CancellationToken cancellationToken = default)
            => _handler(request, cancellationToken);
    }

    /// <summary>Accepts only a fixed credential; principal is the UTF-8 credential text itself.</summary>
    internal sealed class FixedCredentialAuthenticator : IAutoServiceStrictAuthenticator
    {
        private readonly byte[] _expectedCredential;

        internal FixedCredentialAuthenticator(byte[] expectedCredential)
        {
            _expectedCredential = expectedCredential;
        }

        public ValueTask<AutoServiceAuthenticationOutcome> AuthenticateAsync(ReadOnlyMemory<byte> credential, CancellationToken cancellationToken = default)
        {
            if (credential.Span.SequenceEqual(_expectedCredential))
            {
                var principal = System.Text.Encoding.UTF8.GetString(credential.Span);
                return new ValueTask<AutoServiceAuthenticationOutcome>(AutoServiceAuthenticationOutcome.Accepted(ReadOnlyMemory<byte>.Empty, principal));
            }

            return new ValueTask<AutoServiceAuthenticationOutcome>(AutoServiceAuthenticationOutcome.Refused("bad credential"));
        }
    }
}
