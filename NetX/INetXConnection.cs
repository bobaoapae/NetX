using System;
using System.Threading;
using System.Threading.Tasks;

namespace NetX
{
    public interface INetXConnection
    {
        bool IsConnected { get; }

        ValueTask SendAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default);

        /// <summary>
        /// Sends a duplex request and awaits the reply. The returned <see cref="NetXMessage"/> owns a
        /// pooled buffer — the caller MUST dispose it (see <see cref="NetXMessage"/> for the ownership
        /// contract) once done reading <see cref="NetXMessage.Buffer"/>.
        /// </summary>
        Task<NetXMessage> RequestAsync(ReadOnlyMemory<byte> buffer, TimeSpan timeout, CancellationToken cancellationToken = default);
        Task<NetXMessage> RequestAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default);

        ValueTask ReplyAsync(ulong correlationId, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default);

        void Disconnect();
    }
}
