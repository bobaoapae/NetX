using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using CommunityToolkit.HighPerformance.Buffers;
using Microsoft.Extensions.Logging;
using NetX.Options;

namespace NetX
{
    public abstract class NetXConnection : INetXConnection
    {
        public bool IsConnected => DisconnectReason == DisconnectReason.NONE && (_socket?.Connected ?? false);

        internal DisconnectReason DisconnectReason
        {
            get { return _disconnectReason; }
        }

        protected readonly Socket _socket;
        protected readonly NetXConnectionOptions _options;

        protected readonly string _appName;
        protected readonly ILogger _logger;

        private readonly Pipe _sendPipe;
        private readonly Pipe _receivePipe;
        private readonly ConcurrentDictionary<ulong, TaskCompletionSource<NetXMessage>> _completions;

        private readonly CancellationTokenSource _connCancellationTokenSource;

        private readonly bool _reuseSocket;

        private bool _isSocketDisconnectCalled;
        private DisconnectReason _disconnectReason;

        private readonly SemaphoreSlim _semaphore;

        private long _correlationCounter;

        // Frame: [i32 totalLength][u64 correlationId][payload]
        // totalLength is self-inclusive: it counts the whole frame on the wire, including its own 4 bytes.
        // correlationId == 0 means push (no reply expected).
        private const int LENGTH_LEN = sizeof(int);
        private const int CORRELATION_LEN = sizeof(ulong);
        private const int HEADER_LEN = LENGTH_LEN + CORRELATION_LEN;
        private const ulong PUSH_CORRELATION_ID = 0;

        // Directional correlation-id namespaces. Both peers on a connection run their own independent
        // counter starting at 1 — with no namespace split, a client-initiated request and a
        // server-initiated request happening at the same moment can land on the exact same id, and
        // whichever side's reply arrives first gets matched against the WRONG pending completion
        // (a false reply — silent data corruption, not a crash). To make ids unambiguous without any
        // cross-peer coordination, each role claims a disjoint parity: the role that dials out
        // (NetXClient) only ever generates odd ids, the role that accepts (NetXSession) only ever
        // generates even ids; both step by 2. An incoming non-zero correlationId can then be
        // classified purely by its parity, no dictionary lookup required to tell the two cases apart:
        //   - parity == our own role's parity  -> this id space is ours; it must be a reply to one of
        //     OUR outstanding requests (or a stale reply for one that already timed out/was cancelled
        //     — safe to drop, since a genuine peer-initiated request could never land here).
        //   - parity == the other role's parity -> the peer generated this id for a request of ITS
        //     own; dispatch to the message handler, which is expected to ReplyAsync with the same id.
        // This is also what lets the previous timed-out-completions tombstone set be deleted outright:
        // an id in our own namespace that isn't in _completions is unambiguously stale, no bookkeeping
        // required to prove it isn't secretly a fresh peer request.
        private readonly ulong _localCorrelationParity;

        public NetXConnection(Socket socket, NetXConnectionOptions options, string name, ILogger logger, bool isClientRole, bool reuseSocket = false)
        {
            _socket = socket;
            _options = options;

            _appName = name;
            _logger = logger;

            _sendPipe = new Pipe();
            _receivePipe = new Pipe();
            _completions = new ConcurrentDictionary<ulong, TaskCompletionSource<NetXMessage>>();

            _connCancellationTokenSource = new CancellationTokenSource();

            _reuseSocket = reuseSocket;

            _semaphore = new SemaphoreSlim(1, 1);

            // Client counter starts at -1 so the first Add(2) yields 1 (odd); server counter starts
            // at 0 so the first Add(2) yields 2 (even). See _localCorrelationParity remarks above.
            _correlationCounter = isClientRole ? -1 : 0;
            _localCorrelationParity = isClientRole ? 1UL : 0UL;

            socket.NoDelay = _options.NoDelay;
            socket.LingerState = new LingerOption(true, 5);
            socket.ReceiveTimeout = _options.SocketTimeout;
            socket.SendTimeout = _options.SocketTimeout;
            socket.ReceiveBufferSize = _options.RecvBufferSize;
            socket.SendBufferSize = _options.SendBufferSize;

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer, _options.RecvBufferSize);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendBuffer, _options.SendBufferSize);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, _options.SocketTimeout);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout, _options.SocketTimeout);
        }

        #region Send Methods

        public async ValueTask SendAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                WriteFrame(PUSH_CORRELATION_ID, buffer.Span);

                if (!_connCancellationTokenSource.IsCancellationRequested)
                    await _sendPipe.Writer.FlushAsync(_connCancellationTokenSource.Token);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        /// <summary>
        /// Sends a duplex request and awaits the reply. The returned <see cref="NetXMessage"/> owns a
        /// pooled buffer — the caller must dispose it (see <see cref="NetXMessage"/>).
        /// </summary>
        public async Task<NetXMessage> RequestAsync(ReadOnlyMemory<byte> buffer, TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();

            var correlationId = NextCorrelationId();
            var completion =
                new TaskCompletionSource<NetXMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_completions.TryAdd(correlationId, completion))
                throw new Exception($"Cannot track completion for CorrelationId = {correlationId}");

            try
            {
                await _semaphore.WaitAsync(cancellationToken);
                try
                {
                    WriteFrame(correlationId, buffer.Span);

                    if (!_connCancellationTokenSource.IsCancellationRequested)
                        await _sendPipe.Writer.FlushAsync(_connCancellationTokenSource.Token);
                }
                finally
                {
                    _semaphore.Release();
                }
            }
            catch
            {
                // The frame never made it onto the wire (cancelled while waiting for the semaphore,
                // or the flush itself was cancelled/failed) — WaitForRequestAsync, which is the only
                // other place that removes this entry, is never reached below. Without this, the
                // completion is orphaned in _completions for the lifetime of the connection.
                _completions.TryRemove(correlationId, out _);
                throw;
            }

            return await WaitForRequestAsync(correlationId, completion, timeout, cancellationToken);
        }

        public Task<NetXMessage> RequestAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            return RequestAsync(buffer, TimeSpan.Zero, cancellationToken);
        }

        private ulong NextCorrelationId()
        {
            // Interlocked, stepping by 2 to stay within our role's parity — see
            // _localCorrelationParity remarks for why this must not overlap the peer's ids.
            return unchecked((ulong)Interlocked.Add(ref _correlationCounter, 2));
        }

        private bool IsLocalCorrelationId(ulong correlationId) => (correlationId & 1UL) == _localCorrelationParity;

        private Task<NetXMessage> WaitForRequestAsync(ulong correlationId, TaskCompletionSource<NetXMessage> source, TimeSpan timeout, CancellationToken cancellationToken)
        {
            // Determine which timeout to use
            var effectiveTimeout = timeout;

            if (timeout == TimeSpan.Zero)
            {
                effectiveTimeout = TimeSpan.FromMilliseconds(_options.DuplexTimeout);
            }

            CancellationTokenSource innerTimeoutCts = null;
            CancellationTokenSource timeoutCancellation;

            if (effectiveTimeout == Timeout.InfiniteTimeSpan)
            {
                timeoutCancellation = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken, _connCancellationTokenSource.Token);
            }
            else
            {
                innerTimeoutCts = new CancellationTokenSource(effectiveTimeout);
                timeoutCancellation = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken, innerTimeoutCts.Token, _connCancellationTokenSource.Token);
            }

            timeoutCancellation.Token.Register(() =>
            {
                if (source.Task.IsCompleted)
                    return;

                // Set appropriate exception based on which token triggered the cancellation
                if (effectiveTimeout != Timeout.InfiniteTimeSpan
                    && !cancellationToken.IsCancellationRequested
                    && !_connCancellationTokenSource.IsCancellationRequested)
                    source.TrySetException(new TimeoutException());
                else
                    source.TrySetException(new OperationCanceledException(cancellationToken));

                // Just drop our own tracking entry. A reply that arrives after this point falls into
                // the "local-parity id with no matching completion" case in ReadPipeAsync and is
                // dropped there as stale — no separate tombstone set needed to make that safe (see
                // _localCorrelationParity remarks).
                if (!_completions.TryRemove(correlationId, out _))
                    _logger?.LogError("{svrName}: Cannot remove task completion for CorrelationId = {corrId} after timeout", _appName, correlationId);

                // Only disconnect on actual timeout, not on regular cancellation or connection close
                if (_options.DisconnectOnTimeout && effectiveTimeout != Timeout.InfiniteTimeSpan
                    && !cancellationToken.IsCancellationRequested
                    && !_connCancellationTokenSource.IsCancellationRequested)
                    Disconnect();
            });

            // Dispose CTS objects when the task completes (success, timeout, or cancellation)
            source.Task.ContinueWith(_ =>
            {
                timeoutCancellation.Dispose();
                innerTimeoutCts?.Dispose();
            }, TaskScheduler.Default);

            return source.Task;
        }

        public async ValueTask ReplyAsync(ulong correlationId, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (correlationId == PUSH_CORRELATION_ID)
                throw new ArgumentOutOfRangeException(nameof(correlationId), "Cannot reply with the push correlation id (0)");

            if (cancellationToken.IsCancellationRequested)
                return;

            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                WriteFrame(correlationId, buffer.Span);

                if (!_connCancellationTokenSource.IsCancellationRequested)
                    await _sendPipe.Writer.FlushAsync(_connCancellationTokenSource.Token);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        /// <summary>
        /// Writes [i32 totalLength][u64 correlationId][payload] into the send pipe.
        /// Caller must hold <see cref="_semaphore"/>.
        /// </summary>
        private void WriteFrame(ulong correlationId, ReadOnlySpan<byte> payload)
        {
            var totalLength = HEADER_LEN + payload.Length;

            var header = _sendPipe.Writer.GetSpan(HEADER_LEN);
            BinaryPrimitives.WriteInt32LittleEndian(header, totalLength);
            BinaryPrimitives.WriteUInt64LittleEndian(header[LENGTH_LEN..], correlationId);
            _sendPipe.Writer.Advance(HEADER_LEN);

            if (payload.Length > 0)
            {
                var memory = _sendPipe.Writer.GetSpan(payload.Length);
                payload.CopyTo(memory);
                _sendPipe.Writer.Advance(payload.Length);
            }
        }

        #endregion

        internal async Task ProcessConnection(CancellationToken listenCancellationToken = default)
        {
            if (_connCancellationTokenSource.IsCancellationRequested)
                return;

            var listenRegistration = listenCancellationToken.Register(() =>
            {
                if (_disconnectReason == DisconnectReason.NONE)
                    _disconnectReason = DisconnectReason.SHUTDOWN;

                Disconnect();
            });

            try
            {
                var writing = FillPipeAsync(_connCancellationTokenSource.Token);
                var reading = ReadPipeAsync(_connCancellationTokenSource.Token);
                var sending = SendPipeAsync(_connCancellationTokenSource.Token);

                // Wait for receive-side loops to complete first
                await Task.WhenAll(writing, reading);

                // Ensure connection cancellation is triggered so SendPipeAsync can exit
                // (e.g., after REMOTE_CLOSE where FillPipeAsync doesn't cancel)
                if (!_connCancellationTokenSource.IsCancellationRequested)
                    _connCancellationTokenSource.Cancel();

                await sending;

                // Cancel all pending request completions — fail-fast on disconnect
                foreach (var kvp in _completions)
                {
                    if (_completions.TryRemove(kvp.Key, out var tcs))
                        tcs.TrySetException(new OperationCanceledException("Connection closed"));
                }

                if (_disconnectReason == DisconnectReason.NONE)
                    _disconnectReason = DisconnectReason.CLOSE;

                Disconnect();
            }
            finally
            {
                // Dispose registration to release reference from server's CTS to this connection.
                // Without this, every session that ever connected keeps a callback registered
                // on the server's CancellationToken, leaking session objects until server shutdown.
                listenRegistration.Dispose();
                // Note: _connCancellationTokenSource and _semaphore are NOT disposed here
                // because fire-and-forget handlers may still reference them after ProcessConnection exits.
                // They are lightweight and GC-safe.
            }
        }

        public void Disconnect()
        {
            lock (_socket)
            {
                if (_isSocketDisconnectCalled)
                    return;

                _isSocketDisconnectCalled = true;

                if (_disconnectReason == DisconnectReason.NONE)
                    _disconnectReason = DisconnectReason.FORCE;

                _connCancellationTokenSource.Cancel();

                try
                {
                    _socket.Shutdown(SocketShutdown.Both);
                }
                catch (Exception ex)
                {
                    _logger?.LogDebug("{appName}: Exception during socket shutdown: {ex}", _appName, ex);
                }

                try
                {
                    _socket.Disconnect(_reuseSocket);
                }
                catch (Exception ex)
                {
                    _logger?.LogDebug("{appName}: Exception during socket disconnect: {ex}", _appName, ex);
                }

                if (!_reuseSocket)
                {
                    try
                    {
                        _socket.Close();
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogDebug("{appName}: Exception during socket close: {ex}", _appName, ex);
                    }
                }
            }
        }

        private async Task FillPipeAsync(CancellationToken cancellationToken)
        {
            const int minimumBufferSize = 512;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    // Allocate at least 512 bytes from the PipeWriter.
                    Memory<byte> memory = _receivePipe.Writer.GetMemory(minimumBufferSize);

                    int bytesRead = await _socket.ReceiveAsync(memory, SocketFlags.None, cancellationToken);
                    if (bytesRead == 0)
                    {
                        _disconnectReason = DisconnectReason.REMOTE_CLOSE;
                        break;
                    }

                    // Tell the PipeWriter how much was read from the Socket.
                    _receivePipe.Writer.Advance(bytesRead);

                    // Make the data available to the PipeReader.
                    FlushResult result = await _receivePipe.Writer.FlushAsync(cancellationToken);

                    if (result.IsCanceled || result.IsCompleted)
                    {
                        break;
                    }
                }
            }
            catch (SocketException ex)
            {
                _logger?.LogError("{appName}: SocketException in FillPipeAsync: {ex}", _appName, ex);
                _disconnectReason = DisconnectReason.ERROR;
                _connCancellationTokenSource.Cancel();
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                await _receivePipe.Writer.CompleteAsync();
            }
        }

        private async Task ReadPipeAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ReadResult result = await _receivePipe.Reader.ReadAsync(cancellationToken);
                    ReadOnlySequence<byte> buffer = result.Buffer;

                    while (!cancellationToken.IsCancellationRequested && TryGetReceivedMessage(ref buffer, out var message))
                    {
                        if (message.CorrelationId != PUSH_CORRELATION_ID && IsLocalCorrelationId(message.CorrelationId))
                        {
                            // This id is in our own namespace — it can only be a reply to one of OUR
                            // outstanding requests, or a stale reply for one that already timed out /
                            // was cancelled (never a genuine peer-initiated request; see
                            // _localCorrelationParity remarks).
                            if (_completions.TryRemove(message.CorrelationId, out var completion))
                            {
                                // Ownership of the underlying pooled buffer transfers to the request's
                                // awaiter here — it is intentionally NOT disposed on this path (that would
                                // return the array to the pool while the caller still holds a reference to it).
                                if (!completion.TrySetResult(message))
                                {
                                    _logger?.LogError("{appName}: Failed to set duplex completion result. CorrelationId = {corrId}", _appName, message.CorrelationId);
                                    message.Dispose();
                                }
                            }
                            else
                            {
                                // Stale reply — completion already consumed/removed by a timeout or
                                // cancellation. Discard silently instead of dispatching to the handler
                                // as if it were a regular/peer-initiated message.
                                message.Dispose();
                            }

                            continue;
                        }

                        // Either a push (correlationId == 0) or a genuine request from the peer's own
                        // namespace — dispatch to the handler. Ownership of the message transfers to
                        // the handler, which owns disposing it (it may hand it off to background work
                        // that outlives this call, so we must not dispose here — see NetXMessage).
                        //
                        // Bug 6: Isolate handler exceptions per-message.
                        // A single handler failure should not kill the entire IPC connection.
                        try
                        {
                            await OnReceivedMessageAsync(message, cancellationToken);
                        }
                        catch (OperationCanceledException) { throw; }
                        catch (Exception ex)
                        {
                            _logger?.LogError("{appName}: Exception in message handler: {ex}", _appName, ex);
                        }

                        if (result.IsCanceled || result.IsCompleted)
                            break;
                    }

                    if (result.IsCanceled || result.IsCompleted)
                        break;

                    if (_connCancellationTokenSource.IsCancellationRequested)
                        break;

                    _receivePipe.Reader.AdvanceTo(buffer.Start, buffer.End);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                _logger?.LogError("{appName}: Exception in ReadPipeAsync: {ex}", _appName, ex);
                _disconnectReason = DisconnectReason.ERROR;
                _connCancellationTokenSource.Cancel();
            }
            finally
            {
                await _receivePipe.Reader.CompleteAsync();
            }
        }

        /// <summary>
        /// Parses one frame out of the accumulated receive buffer, if a full frame is available.
        /// The Pipe itself is the accumulation buffer — a frame that spans multiple socket reads
        /// simply waits here (returns false) until FillPipeAsync has delivered enough bytes; there is
        /// no dependency on RecvBufferSize. Frames whose payload would exceed <see cref="NetXConnectionOptions.MaxFrameBytes"/>
        /// are treated as a protocol violation and the connection is dropped.
        /// The payload is copied exactly once: straight from the pipe's <see cref="ReadOnlySequence{T}"/>
        /// into a freshly rented <see cref="MemoryOwner{T}"/> that is handed to the caller (ownership
        /// transfers with the returned <see cref="NetXMessage"/> — the caller must Dispose it).
        /// </summary>
        private bool TryGetReceivedMessage(ref ReadOnlySequence<byte> buffer, out NetXMessage netXMessage)
        {
            netXMessage = default;

            if (buffer.Length < HEADER_LEN)
                return false;

            Span<byte> header = stackalloc byte[HEADER_LEN];
            buffer.Slice(0, HEADER_LEN).CopyTo(header);

            var totalLength = BinaryPrimitives.ReadInt32LittleEndian(header);
            var correlationId = BinaryPrimitives.ReadUInt64LittleEndian(header[LENGTH_LEN..]);

            if (totalLength < HEADER_LEN)
            {
                _logger?.LogError(
                    "{appName}: Invalid frame length {size}, expected >= {headerLen}. Disconnecting.",
                    _appName, totalLength, HEADER_LEN);
                _disconnectReason = DisconnectReason.CLOSE;
                _connCancellationTokenSource.Cancel();
                return false;
            }

            var payloadLength = totalLength - HEADER_LEN;

            if (payloadLength > _options.MaxFrameBytes)
            {
                _logger?.LogError(
                    "{appName}: Frame payload {payloadLength} exceeds MaxFrameBytes {maxFrameBytes}. Disconnecting.",
                    _appName, payloadLength, _options.MaxFrameBytes);
                _disconnectReason = DisconnectReason.CLOSE;
                _connCancellationTokenSource.Cancel();
                return false;
            }

            if (totalLength > buffer.Length)
                // Frame not fully received yet — wait for more data to accumulate in the pipe.
                return false;

            var payloadOwner = MemoryOwner<byte>.Allocate(payloadLength);
            if (payloadLength > 0)
                buffer.Slice(HEADER_LEN, payloadLength).CopyTo(payloadOwner.Span);

            ProcessReceivedBuffer(payloadOwner.Memory);

            var next = buffer.GetPosition(totalLength);
            buffer = buffer.Slice(next);

            netXMessage = new NetXMessage(correlationId, payloadOwner);

            return true;
        }

        private async Task SendPipeAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ReadResult result = await _sendPipe.Reader.ReadAsync(cancellationToken);
                    ReadOnlySequence<byte> buffer = result.Buffer;

                    if (result.IsCanceled || result.IsCompleted)
                        break;

                    while (!cancellationToken.IsCancellationRequested && TryGetSendFrame(ref buffer, out var frame))
                    {
                        if (_socket.Connected)
                        {
                            // Stream the frame straight out of the pipe's own (possibly multi-segment)
                            // buffer — no intermediate scratch buffer, so a frame's validity/size no
                            // longer depends on NetXConnectionOptions.SendBufferSize (that option only
                            // sizes the OS-level socket send buffer now).
                            foreach (var segment in frame)
                            {
                                if (segment.IsEmpty)
                                    continue;

                                ProcessSendBuffer(in segment);
                                await SendSegmentAsync(segment, cancellationToken);
                            }
                        }
                    }

                    _sendPipe.Reader.AdvanceTo(buffer.Start, buffer.End);
                }
            }
            catch (SocketException ex)
            {
                _logger?.LogError("{appName}: SocketException in SendPipeAsync: {ex}", _appName, ex);
                _disconnectReason = DisconnectReason.ERROR;
                _connCancellationTokenSource.Cancel();
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                _logger?.LogError("{appName}: Exception in SendPipeAsync: {ex}", _appName, ex);
                _disconnectReason = DisconnectReason.ERROR;
                _connCancellationTokenSource.Cancel();
            }
            finally
            {
                await _sendPipe.Reader.CompleteAsync();
            }
        }

        /// <summary>
        /// Sends one contiguous pipe segment to completion. <see cref="Socket.SendAsync(Memory{byte}, SocketFlags, CancellationToken)"/>
        /// is free to write fewer bytes than requested (a partial send) — advance the offset by
        /// exactly what the OS accepted and keep sending the remainder, instead of assuming the whole
        /// segment always goes out in a single call.
        /// </summary>
        private async Task SendSegmentAsync(ReadOnlyMemory<byte> segment, CancellationToken cancellationToken)
        {
            var offset = 0;
            while (offset < segment.Length)
            {
                using var sendCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                sendCts.CancelAfter(_options.SocketTimeout > 0 ? _options.SocketTimeout : 3000);

                int sent;
                try
                {
                    sent = await _socket.SendAsync(segment[offset..], SocketFlags.None, sendCts.Token);
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    throw new SocketException((int)SocketError.TimedOut);
                }

                if (sent <= 0)
                    throw new SocketException((int)SocketError.ConnectionReset);

                offset += sent;
            }
        }

        /// <summary>
        /// Slices exactly one complete frame off the front of the accumulated send buffer, if one is
        /// fully available. Does not copy — the returned <see cref="ReadOnlySequence{T}"/> is a view
        /// over the pipe's own buffered segments.
        /// </summary>
        private bool TryGetSendFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> frame)
        {
            frame = default;

            if (buffer.Length < LENGTH_LEN)
                return false;

            Span<byte> lengthSpan = stackalloc byte[LENGTH_LEN];
            buffer.Slice(0, LENGTH_LEN).CopyTo(lengthSpan);
            var size = BinaryPrimitives.ReadInt32LittleEndian(lengthSpan);

            if (size > buffer.Length)
                // Frame not fully buffered yet — WriteFrame always writes a complete frame while
                // holding _semaphore, so this just means the writer's FlushAsync hasn't caught up yet.
                return false;

            frame = buffer.Slice(0, size);
            buffer = buffer.Slice(size);

            return true;
        }

        protected virtual void ProcessReceivedBuffer(in ReadOnlyMemory<byte> buffer)
        {
        }

        protected virtual void ProcessSendBuffer(in ReadOnlyMemory<byte> buffer)
        {
        }

        protected abstract ValueTask OnReceivedMessageAsync(NetXMessage message, CancellationToken cancellationToken);
    }
}
