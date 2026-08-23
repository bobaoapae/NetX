using System;
using System.Threading;
using System.Threading.Tasks;
using NetX.AutoServiceGenerator.Definitions;

namespace NetX.AutoService.Internal
{
    /// <summary>Default authenticator used when a builder does not configure one: accepts unconditionally.</summary>
    internal sealed class AutoServiceNoAuthAuthenticator : IAutoServiceStrictAuthenticator
    {
        internal static readonly AutoServiceNoAuthAuthenticator Instance = new();

        private AutoServiceNoAuthAuthenticator()
        {
        }

        public ValueTask<AutoServiceAuthenticationOutcome> AuthenticateAsync(ReadOnlyMemory<byte> credential, CancellationToken cancellationToken = default)
            => new(AutoServiceAuthenticationOutcome.Accepted(ReadOnlyMemory<byte>.Empty));
    }
}
