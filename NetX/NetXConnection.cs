using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Runtime.InteropServices;
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
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<ArraySegment<byte>>> _completions;
        private readonly ConcurrentDictionary<Guid, byte> _timedOutCompletions;

        private readonly CancellationTokenSource _connCancellationTokenSource;

        private readonly bool _reuseSocket;

        private bool _isSocketDisconnectCalled;
        private DisconnectReason _disconnectReason;

        private readonly SemaphoreSlim _semaphore;

        const int GUID_LEN = 16;
        private static readonly byte[] _emptyGuid = Guid.Empty.ToByteArray();


        public NetXConnection(Socket socket, NetXConnectionOptions options, string name, ILogger logger, bool reuseSocket = false)
        {
            _socket = socket;
            _options = options;

            _appName = name;
            _logger = logger;

            _sendPipe = new Pipe();
            _receivePipe = new Pipe();
            _completions = new ConcurrentDictionary<Guid, TaskCompletionSource<ArraySegment<byte>>>();
            _timedOutCompletions = new ConcurrentDictionary<Guid, byte>();

            _connCancellationTokenSource = new CancellationTokenSource();

            _reuseSocket = reuseSocket;

            _semaphore = new SemaphoreSlim(1, 1);

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

        public async ValueTask SendAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                var size = buffer.Count + (_options.Duplex ? sizeof(int) + GUID_LEN : 0);
                BitConverter.TryWriteBytes(_sendPipe.Writer.GetSpan(sizeof(int)), size);
                _sendPipe.Writer.Advance(sizeof(int));

                if (_options.Duplex)
                {
                    _sendPipe.Writer.Write(_emptyGuid);
                }

                var memory = _sendPipe.Writer.GetMemory(buffer.Count);
                buffer.AsMemory().CopyTo(memory);

                _sendPipe.Writer.Advance(buffer.Count);

                if (!_connCancellationTokenSource.IsCancellationRequested)
                    await _sendPipe.Writer.FlushAsync(_connCancellationTokenSource.Token);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async ValueTask SendAsync(Stream stream, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                stream.Position = 0;
                var streamLength = (int)stream.Length;

                // Read stream into temporary buffer BEFORE writing to the pipe.
                // If the read fails (IOException, etc.), the pipe is untouched — no corruption.
                var rentedBuffer = ArrayPool<byte>.Shared.Rent(streamLength);
                int bytesRead;
                try
                {
                    bytesRead = await stream.ReadAsync(rentedBuffer.AsMemory(0, streamLength), cancellationToken);
                }
                catch
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
                    throw;
                }

                try
                {
                    if (bytesRead != 0)
                    {
                        var size = bytesRead + (_options.Duplex ? sizeof(int) + GUID_LEN : 0);
                        BitConverter.TryWriteBytes(_sendPipe.Writer.GetSpan(sizeof(int)), size);
                        _sendPipe.Writer.Advance(sizeof(int));

                        if (_options.Duplex)
                        {
                            _sendPipe.Writer.Write(_emptyGuid);
                        }

                        var memory = _sendPipe.Writer.GetMemory(bytesRead);
                        rentedBuffer.AsMemory(0, bytesRead).CopyTo(memory);
                        _sendPipe.Writer.Advance(bytesRead);

                        if (!_connCancellationTokenSource.IsCancellationRequested)
                            await _sendPipe.Writer.FlushAsync(_connCancellationTokenSource.Token);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task<ArraySegment<byte>> RequestAsync(ArraySegment<byte> buffer, TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            if (!_options.Duplex)
                throw new NotSupportedException(
                    $"Cannot use RequestAsync with {nameof(_options.Duplex)} option disabled");

            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();

            var messageId = Guid.NewGuid();
            var completion =
                new TaskCompletionSource<ArraySegment<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_completions.TryAdd(messageId, completion))
                throw new Exception($"Cannot track completion for MessageId = {messageId}");

            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                var size = buffer.Count + sizeof(int) + GUID_LEN;
                BitConverter.TryWriteBytes(_sendPipe.Writer.GetSpan(sizeof(int)), size);
                _sendPipe.Writer.Advance(sizeof(int));

                messageId.TryWriteBytes(_sendPipe.Writer.GetSpan(GUID_LEN));
                _sendPipe.Writer.Advance(GUID_LEN);

                var memory = _sendPipe.Writer.GetMemory(buffer.Count);
                buffer.AsMemory().CopyTo(memory);

                _sendPipe.Writer.Advance(buffer.Count);

                if (!_connCancellationTokenSource.IsCancellationRequested)
                    await _sendPipe.Writer.FlushAsync(_connCancellationTokenSource.Token);
            }
            finally
            {
                _semaphore.Release();
            }

            return await WaitForRequestAsync(messageId, completion, timeout, cancellationToken);
        }

        public async Task<ArraySegment<byte>> RequestAsync(Stream stream, TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            if (!_options.Duplex)
                throw new NotSupportedException($"Cannot use RequestAsync with {nameof(_options.Duplex)} option disabled");

            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();

            var messageId = Guid.NewGuid();
            var completion = new TaskCompletionSource<ArraySegment<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_completions.TryAdd(messageId, completion))
                throw new Exception($"Cannot track completion for MessageId = {messageId}");

            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                stream.Position = 0;
                var streamLength = (int)stream.Length;

                // Read stream into temporary buffer BEFORE writing to the pipe.
                var rentedBuffer = ArrayPool<byte>.Shared.Rent(streamLength);
                int bytesRead;
                try
                {
                    bytesRead = await stream.ReadAsync(rentedBuffer.AsMemory(0, streamLength), cancellationToken);
                }
                catch
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
                    throw;
                }

                try
                {
                    if (bytesRead != 0)
                    {
                        var size = bytesRead + sizeof(int) + GUID_LEN;
                        BitConverter.TryWriteBytes(_sendPipe.Writer.GetSpan(sizeof(int)), size);
                        _sendPipe.Writer.Advance(sizeof(int));

                        messageId.TryWriteBytes(_sendPipe.Writer.GetSpan(GUID_LEN));
                        _sendPipe.Writer.Advance(GUID_LEN);

                        var memory = _sendPipe.Writer.GetMemory(bytesRead);
                        rentedBuffer.AsMemory(0, bytesRead).CopyTo(memory);
                        _sendPipe.Writer.Advance(bytesRead);

                        if (!_connCancellationTokenSource.IsCancellationRequested)
                            await _sendPipe.Writer.FlushAsync(_connCancellationTokenSource.Token);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
                }
            }
            finally
            {
                _semaphore.Release();
            }

            return await WaitForRequestAsync(messageId, completion, timeout, cancellationToken);
        }

        public Task<ArraySegment<byte>> RequestAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken = default)
        {
            return RequestAsync(buffer, TimeSpan.Zero, cancellationToken);
        }

        public Task<ArraySegment<byte>> RequestAsync(Stream stream, CancellationToken cancellationToken = default)
        {
            return RequestAsync(stream, TimeSpan.Zero, cancellationToken);
        }

        private Task<ArraySegment<byte>> WaitForRequestAsync(Guid taskCompletionId, TaskCompletionSource<ArraySegment<byte>> source, TimeSpan timeout, CancellationToken cancellationToken)
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

                if (_completions.TryRemove(taskCompletionId, out var __))
                {
                    _timedOutCompletions.TryAdd(taskCompletionId, 0);
                }
                else
                {
                    _logger?.LogError("{svrName}: Cannot remove task completion for MessageId = {msgId} after timeout", _appName, taskCompletionId);
                }

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

        public async ValueTask ReplyAsync(Guid messageId, ArraySegment<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (!_options.Duplex)
                throw new NotSupportedException(
                    $"Cannot use ReplyAsync with {nameof(_options.Duplex)} option disabled");

            if (cancellationToken.IsCancellationRequested)
                return;

            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                var size = buffer.Count + sizeof(int) + GUID_LEN;
                BitConverter.TryWriteBytes(_sendPipe.Writer.GetSpan(sizeof(int)), size);
                _sendPipe.Writer.Advance(sizeof(int));

                messageId.TryWriteBytes(_sendPipe.Writer.GetSpan(GUID_LEN));
                _sendPipe.Writer.Advance(GUID_LEN);

                var memory = _sendPipe.Writer.GetMemory(buffer.Count);
                buffer.AsMemory().CopyTo(memory);

                _sendPipe.Writer.Advance(buffer.Count);

                if (!_connCancellationTokenSource.IsCancellationRequested)
                    await _sendPipe.Writer.FlushAsync(_connCancellationTokenSource.Token);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async ValueTask ReplyAsync(Guid messageId, Stream stream, CancellationToken cancellationToken = default)
        {
            if (!_options.Duplex)
                throw new NotSupportedException(
                    $"Cannot use ReplyAsync with {nameof(_options.Duplex)} option disabled");

            if (cancellationToken.IsCancellationRequested)
                return;

            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                stream.Position = 0;
                var streamLength = (int)stream.Length;

                // Read stream into temporary buffer BEFORE writing to the pipe.
                var rentedBuffer = ArrayPool<byte>.Shared.Rent(streamLength);
                int bytesRead;
                try
                {
                    bytesRead = await stream.ReadAsync(rentedBuffer.AsMemory(0, streamLength), cancellationToken);
                }
                catch
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
                    throw;
                }

                try
                {
                    if (bytesRead != 0)
                    {
                        var size = bytesRead + sizeof(int) + GUID_LEN;
                        BitConverter.TryWriteBytes(_sendPipe.Writer.GetSpan(sizeof(int)), size);
                        _sendPipe.Writer.Advance(sizeof(int));

                        messageId.TryWriteBytes(_sendPipe.Writer.GetSpan(GUID_LEN));
                        _sendPipe.Writer.Advance(GUID_LEN);

                        var memory = _sendPipe.Writer.GetMemory(bytesRead);
                        rentedBuffer.AsMemory(0, bytesRead).CopyTo(memory);
                        _sendPipe.Writer.Advance(bytesRead);

                        if (!_connCancellationTokenSource.IsCancellationRequested)
                            await _sendPipe.Writer.FlushAsync(_connCancellationTokenSource.Token);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
                }
            }
            finally
            {
                _semaphore.Release();
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
                using var recvBuffer = MemoryOwner<byte>.Allocate(_options.RecvBufferSize);
                using var sendBuffer = MemoryOwner<byte>.Allocate(_options.SendBufferSize);

                var writing = FillPipeAsync(_connCancellationTokenSource.Token);
                var reading = ReadPipeAsync(recvBuffer, _connCancellationTokenSource.Token);
                var sending = SendPipeAsync(sendBuffer, _connCancellationTokenSource.Token);

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
                // Free timed-out completions tracking memory
                _timedOutCompletions.Clear();
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

        private async Task ReadPipeAsync(MemoryOwner<byte> recvBuffer, CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ReadResult result = await _receivePipe.Reader.ReadAsync(cancellationToken);
                    ReadOnlySequence<byte> buffer = result.Buffer;

                    while (!cancellationToken.IsCancellationRequested && TryGetReceivedMessage(ref buffer, recvBuffer, out var message))
                    {
                        if (_options.Duplex && message.Id != Guid.Empty && _completions.TryRemove(message.Id, out var completion))
                        {
                            if (!MemoryMarshal.TryGetArray(message.Buffer, out var arraySegment))
                            {
                                _logger?.LogError("{appName}: Failed to get array segment from duplex message. MessageId = {msgId}", _appName, message.Id);
                                message.Dispose();
                                continue;
                            }

                            if (!completion.TrySetResult(arraySegment))
                            {
                                _logger?.LogError("{appName}: Failed to set duplex completion result. MessageId = {msgId}", _appName, message.Id);
                                message.Dispose();
                            }

                            continue;
                        }

                        // Bug 7: Stale duplex reply — completion was already consumed by timeout.
                        // Discard silently instead of dispatching to handler as regular message.
                        if (_options.Duplex && message.Id != Guid.Empty
                            && _timedOutCompletions.TryRemove(message.Id, out _))
                        {
                            message.Dispose();
                            continue;
                        }

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

        private bool TryGetReceivedMessage(
            ref ReadOnlySequence<byte> buffer,
            MemoryOwner<byte> recvBuffer,
            out NetXMessage netXMessage)
        {
            netXMessage = default;

            const int DUPLEX_HEADER_SIZE = sizeof(int) + GUID_LEN;

            if (buffer.IsEmpty || (_options.Duplex && buffer.Length < DUPLEX_HEADER_SIZE))
                return false;

            var headerOffset = _options.Duplex ? DUPLEX_HEADER_SIZE : 0;

            var minRecvSize = Math.Min(_options.RecvBufferSize, buffer.Length);
            buffer.Slice(0, _options.Duplex ? headerOffset : minRecvSize).CopyTo(recvBuffer.Span);

            var size = _options.Duplex ? BitConverter.ToInt32(recvBuffer.Span) : GetReceiveMessageSize(recvBuffer.Memory[..(int)minRecvSize]);
            var messageId = _options.Duplex ? new Guid(recvBuffer.Span.Slice(4, 16)) : Guid.Empty;

            if (size <= 0 || (_options.Duplex && size < headerOffset))
            {
                _logger?.LogError(
                    "{appName}: Invalid frame size {size}, expected >= {headerOffset}. Disconnecting.",
                    _appName, size, headerOffset);
                _disconnectReason = DisconnectReason.CLOSE;
                _connCancellationTokenSource.Cancel();
                return false;
            }

            if (size > _options.RecvBufferSize)
                throw new Exception(
                    $"Recv Buffer is too small. RecvBuffLen = {_options.RecvBufferSize} ReceivedLen = {size}");

            if (size > buffer.Length)
                return false;

            buffer.Slice(headerOffset, size - headerOffset).CopyTo(recvBuffer.Span);

            var messageBuffer = recvBuffer.Memory[..(size - headerOffset)];
            ProcessReceivedBuffer(messageBuffer);

            var next = buffer.GetPosition(size);
            buffer = buffer.Slice(next);

            var messageMemory = MemoryOwner<byte>.Allocate(messageBuffer.Length);
            messageBuffer.CopyTo(messageMemory.Memory);

            netXMessage = new NetXMessage(messageId, messageMemory);

            return true;
        }

        private async Task SendPipeAsync(MemoryOwner<byte> sendBuffer, CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ReadResult result = await _sendPipe.Reader.ReadAsync(cancellationToken);
                    ReadOnlySequence<byte> buffer = result.Buffer;

                    if (result.IsCanceled || result.IsCompleted)
                        break;

                    while (!cancellationToken.IsCancellationRequested && TryGetSendMessage(ref buffer, sendBuffer, out var sendBuff))
                    {
                        if (_socket.Connected)
                        {
                            try
                            {
                                using var sendCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                                sendCts.CancelAfter(_options.SocketTimeout > 0 ? _options.SocketTimeout : 3000);
                                await _socket.SendAsync(sendBuff, SocketFlags.None, sendCts.Token);
                            }
                            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                            {
                                throw new SocketException((int)SocketError.TimedOut);
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

        private bool TryGetSendMessage(
            ref ReadOnlySequence<byte> buffer,
            MemoryOwner<byte> sendBuffer,
            out ReadOnlyMemory<byte> sendBuff)
        {
            sendBuff = default;

            var offset = _options.Duplex ? 0 : sizeof(int);

            if (buffer.IsEmpty || buffer.Length < sizeof(int))
                return false;

            buffer.Slice(0, sizeof(int)).CopyTo(sendBuffer.Span);
            var size = BitConverter.ToInt32(sendBuffer.Span[..sizeof(int)]);

            if (size > _options.SendBufferSize)
                throw new Exception($"Send Buffer is too small. SendBuffLen = {_options.SendBufferSize} SendLen = {size}");

            if (size > buffer.Length)
                return false;

            buffer.Slice(offset, size).CopyTo(sendBuffer.Span);

            sendBuff = sendBuffer.Memory[..size];

            ProcessSendBuffer(in sendBuff);

            var next = buffer.GetPosition(size + offset);
            buffer = buffer.Slice(next);

            return true;
        }

        protected virtual int GetReceiveMessageSize(in ReadOnlyMemory<byte> buffer)
        {
            return 0;
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