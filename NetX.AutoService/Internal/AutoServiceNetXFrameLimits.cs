using System;

namespace NetX.AutoService.Internal
{
    /// <summary>
    /// Shared arithmetic for deriving the NetX-level <c>MaxFrameBytes</c> (the transport's own frame
    /// size guard) from the AutoService-level one a caller configures via
    /// <see cref="AutoServiceNetXServerBuilder.WithMaxFrameBytes"/> / <see cref="AutoServiceNetXClientBuilder.WithMaxFrameBytes"/>.
    /// </summary>
    internal static class AutoServiceNetXFrameLimits
    {
        /// <summary>
        /// Safety headroom given to NetX's own MaxFrameBytes on top of the configured AutoService
        /// MaxFrameBytes, so the small AutoServiceFrame header/error-body overhead never causes NetX's
        /// transport-level guard to reject a frame that is otherwise within the AutoService-level limit.
        /// Comfortably covers the fixed v2 frame header (marker/version/kind/correlation/logical-call-id
        /// /sequence/service-id/schema/fingerprint/operation, all well under 1 KiB even at the strict
        /// service-id length ceiling) plus a short structured error body.
        /// </summary>
        internal const int OverheadMargin = 8 * 1024;

        /// <summary>
        /// Adds <see cref="OverheadMargin"/> to <paramref name="autoServiceMaxFrameBytes"/>, failing
        /// closed instead of silently wrapping when the caller's configured value is close enough to
        /// <see cref="int.MaxValue"/> that the addition would overflow.
        /// </summary>
        internal static int AddOverheadMargin(int autoServiceMaxFrameBytes)
        {
            try
            {
                return checked(autoServiceMaxFrameBytes + OverheadMargin);
            }
            catch (OverflowException)
            {
                // Matches the exception type WithMaxFrameBytes already throws for other invalid values
                // on these builders, instead of surfacing a raw OverflowException from unrelated arithmetic.
                throw new ArgumentOutOfRangeException(
                    nameof(autoServiceMaxFrameBytes),
                    autoServiceMaxFrameBytes,
                    $"MaxFrameBytes is too close to Int32.MaxValue to add the {OverheadMargin}-byte NetX frame overhead margin.");
            }
        }
    }
}
