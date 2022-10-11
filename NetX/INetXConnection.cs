using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace NetX
{
    public interface INetXConnection
    {
        ValueTask SendAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken = default);
        ValueTask SendAsync(Stream stream, CancellationToken cancellationToken = default);

        ValueTask<ArraySegment<byte>> RequestAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken = default);
        ValueTask<ArraySegment<byte>> RequestAsync(Stream stream, CancellationToken cancellationToken = default);

        ValueTask ReplyAsync(Guid messageId, ArraySegment<byte> buffer, CancellationToken cancellationToken = default);
        ValueTask ReplyAsync(Guid messageId, Stream stream, CancellationToken cancellationToken = default);

        void Disconnect();
        bool IsConnected { get; }
    }
}
