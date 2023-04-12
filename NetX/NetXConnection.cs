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
        protected readonly Socket _socket;
        protected readonly NetXConnectionOptions _options;

        protected readonly string _appName;
        protected readonly ILogger _logger;

        private readonly Pipe _sendPipe;
        private readonly Pipe _receivePipe;
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<ArraySegment<byte>>> _completions;

        private CancellationTokenSource _cancellationTokenSource;

        private readonly bool _reuseSocket;
        private bool _isSocketDisconnectCalled;

        private readonly SemaphoreSlim _semaphore;

        const int GUID_LEN = 16;
        private static readonly byte[] _emptyGuid = Guid.Empty.ToByteArray();

        public bool IsConnected => _socket?.Connected ?? false;

        public NetXConnection(Socket socket, NetXConnectionOptions options, string name, ILogger logger, bool reuseSocket = false)
        {
            _socket = socket;
            _options = options;

            _appName = name;
            _logger = logger;

            _sendPipe = new Pipe();
            _receivePipe = new Pipe();
            _completions = new ConcurrentDictionary<Guid, TaskCompletionSource<ArraySegment<byte>>>();

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

                await _sendPipe.Writer.FlushAsync(cancellationToken);
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

                var size = (int)stream.Length + (_options.Duplex ? sizeof(int) + GUID_LEN : 0);
                BitConverter.TryWriteBytes(_sendPipe.Writer.GetSpan(sizeof(int)), size);
                _sendPipe.Writer.Advance(sizeof(int));

                if (_options.Duplex)
                {
                    _sendPipe.Writer.Write(_emptyGuid);
                }

                var memory = _sendPipe.Writer.GetMemory((int)stream.Length);
                var bytesRead = await stream.ReadAsync(memory, cancellationToken);
                if (bytesRead != 0)
                {
                    _sendPipe.Writer.Advance(bytesRead);
                    _ = await _sendPipe.Writer.FlushAsync(cancellationToken);
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task<ArraySegment<byte>> RequestAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken = default)
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

                await _sendPipe.Writer.FlushAsync(cancellationToken);
            }
            finally
            {
                _semaphore.Release();
            }

            return await WaitForRequestAsync(messageId, completion, cancellationToken);
        }

        public async Task<ArraySegment<byte>> RequestAsync(Stream stream, CancellationToken cancellationToken = default)
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

                var size = (int)stream.Length + sizeof(int) + GUID_LEN;
                BitConverter.TryWriteBytes(_sendPipe.Writer.GetSpan(sizeof(int)), size);
                _sendPipe.Writer.Advance(sizeof(int));

                messageId.TryWriteBytes(_sendPipe.Writer.GetSpan(GUID_LEN));
                _sendPipe.Writer.Advance(GUID_LEN);


                var memory = _sendPipe.Writer.GetMemory((int)stream.Length);
                var bytesRead = await stream.ReadAsync(memory, cancellationToken);
                if (bytesRead != 0)
                {
                    _sendPipe.Writer.Advance(bytesRead);
                    await _sendPipe.Writer.FlushAsync(cancellationToken);
                }
            }
            finally
            {
                _semaphore.Release();
            }

            return await WaitForRequestAsync(messageId, completion, cancellationToken);
        }

        private Task<ArraySegment<byte>> WaitForRequestAsync(Guid taskCompletionId, TaskCompletionSource<ArraySegment<byte>> source, CancellationToken cancellationToken)
        {
            var timeoutCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, new CancellationTokenSource(_options.DuplexTimeout).Token);
            timeoutCancellation.Token.Register(() =>
            {
                if (source.Task.IsCompleted)
                    return;

                source.SetException(new TimeoutException());

                if (!_completions.TryRemove(taskCompletionId, out var __))
                {
                    _logger?.LogError("{svrName}: Cannot remove task completion for MessageId = {msgId} after timeout", _appName, taskCompletionId);
                }

                if (_options.DisconnectOnTimeout)
                    Disconnect();
            });
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

                await _sendPipe.Writer.FlushAsync(cancellationToken);
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

                var size = (int)stream.Length + sizeof(int) + GUID_LEN;
                BitConverter.TryWriteBytes(_sendPipe.Writer.GetSpan(sizeof(int)), size);
                _sendPipe.Writer.Advance(sizeof(int));

                messageId.TryWriteBytes(_sendPipe.Writer.GetSpan(GUID_LEN));
                _sendPipe.Writer.Advance(GUID_LEN);

                var memory = _sendPipe.Writer.GetMemory((int)stream.Length);
                var bytesRead = await stream.ReadAsync(memory, cancellationToken);
                if (bytesRead != 0)
                {
                    _sendPipe.Writer.Advance(bytesRead);
                    await _sendPipe.Writer.FlushAsync(cancellationToken);
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        #endregion

        internal async Task ProcessConnection(CancellationToken cancellationToken = default)
        {
            lock (_socket)
            {
                _isSocketDisconnectCalled = false;
            }

            _cancellationTokenSource = new CancellationTokenSource();
            cancellationToken.Register(() => _cancellationTokenSource.Cancel());

            using var recvBuffer = MemoryOwner<byte>.Allocate(_options.RecvBufferSize);
            using var sendBuffer = MemoryOwner<byte>.Allocate(_options.SendBufferSize);

            var writing = FillPipeAsync(_cancellationTokenSource.Token);
            var reading = ReadPipeAsync(recvBuffer, _cancellationTokenSource.Token);
            var sending = SendPipeAsync(sendBuffer, _cancellationTokenSource.Token);

            await Task.WhenAll(writing, reading, sending);
        }

        public void Disconnect()
        {
            if (!_cancellationTokenSource.IsCancellationRequested)
                _cancellationTokenSource.Cancel();

            lock (_socket)
            {
                if (_isSocketDisconnectCalled)
                    return;

                _isSocketDisconnectCalled = true;
                _socket.Shutdown(SocketShutdown.Both);
                _socket.Disconnect(_reuseSocket);
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
            catch (SocketException)
            {
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                await _receivePipe.Writer.CompleteAsync();
                _sendPipe.Reader.CancelPendingRead();
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
                            //If set result fails, it means that the message was received but the completion source was canceled or timed out. So we just log and ignore it.
                            if (!MemoryMarshal.TryGetArray(message.Buffer, out var arraySegment))
                            {
                                _logger?.LogError("{appName}: Failed to get array segment from duplex message. MessageId = {msgId}", _appName, message.Id);
                                continue;
                            }

                            if (!completion.TrySetResult(arraySegment))
                            {
                                _logger?.LogError("{appName}: Failed to set duplex completion result. MessageId = {msgId}", _appName, message.Id);
                            }

                            continue;
                        }

                        await OnReceivedMessageAsync(message, cancellationToken);

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
            finally
            {
                Disconnect();
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

            netXMessage = new NetXMessage(messageId, _options.CopyBuffer ? messageBuffer.ToArray() : messageBuffer);

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
                            await _socket.SendAsync(sendBuff, SocketFlags.None, cancellationToken);
                        }
                    }

                    _sendPipe.Reader.AdvanceTo(buffer.Start, buffer.End);
                }
            }
            catch (SocketException)
            {
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                Disconnect();
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