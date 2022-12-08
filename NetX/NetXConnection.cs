using System.Threading;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.IO.Pipelines;
using System.Buffers;
using System;
using NetX.Options;
using System.Collections.Concurrent;
using System.IO;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;

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

        private byte[] _recvBuffer;
        private byte[] _sendBuffer;
        private CancellationTokenSource _cancellationTokenSource;

        private readonly bool _reuseSocket;
        private bool _isSocketDisconnectCalled;

        private readonly AsyncLock _mutex;

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

            _mutex = new AsyncLock();

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

            using (await _mutex.LockAsync(cancellationToken))
            {
                _sendPipe.Writer.Write(BitConverter.GetBytes(buffer.Count + (_options.Duplex ? sizeof(int) + GUID_LEN : 0)));

                if (_options.Duplex)
                {
                    _sendPipe.Writer.Write(_emptyGuid);
                }

                var memory = _sendPipe.Writer.GetMemory(buffer.Count);
                buffer.AsMemory().CopyTo(memory);

                _sendPipe.Writer.Advance(buffer.Count);

                await _sendPipe.Writer.FlushAsync(cancellationToken);
            }
        }

        public async ValueTask SendAsync(Stream stream, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            using (await _mutex.LockAsync(cancellationToken))
            {
                stream.Position = 0;

                _sendPipe.Writer.Write(BitConverter.GetBytes((int)stream.Length + (_options.Duplex ? sizeof(int) + GUID_LEN : 0)));

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
        }

        public async ValueTask<ArraySegment<byte>> RequestAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (!_options.Duplex)
                throw new NotSupportedException($"Cannot use RequestAsync with {nameof(_options.Duplex)} option disabled");

            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();

            var messageId = Guid.NewGuid();
            var completion = new TaskCompletionSource<ArraySegment<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_completions.TryAdd(messageId, completion))
                throw new Exception($"Cannot track completion for MessageId = {messageId}");

            using (await _mutex.LockAsync(cancellationToken))
            {
                _sendPipe.Writer.Write(BitConverter.GetBytes(buffer.Count + sizeof(int) + GUID_LEN));

                _sendPipe.Writer.Write(messageId.ToByteArray());

                var memory = _sendPipe.Writer.GetMemory(buffer.Count);
                buffer.AsMemory().CopyTo(memory);

                _sendPipe.Writer.Advance(buffer.Count);

                await _sendPipe.Writer.FlushAsync(cancellationToken);
            }

            return await WaitForRequestAsync(messageId, completion, cancellationToken);
        }

        public async ValueTask<ArraySegment<byte>> RequestAsync(Stream stream, CancellationToken cancellationToken = default)
        {
            if (!_options.Duplex)
                throw new NotSupportedException($"Cannot use RequestAsync with {nameof(_options.Duplex)} option disabled");

            if (cancellationToken.IsCancellationRequested)
                throw new OperationCanceledException();

            var messageId = Guid.NewGuid();
            var completion = new TaskCompletionSource<ArraySegment<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_completions.TryAdd(messageId, completion))
                throw new Exception($"Cannot track completion for MessageId = {messageId}");

            using (await _mutex.LockAsync(cancellationToken))
            {
                stream.Position = 0;

                _sendPipe.Writer.Write(BitConverter.GetBytes((int)stream.Length + sizeof(int) + GUID_LEN));

                _sendPipe.Writer.Write(messageId.ToByteArray());

                var memory = _sendPipe.Writer.GetMemory((int)stream.Length);
                var bytesRead = await stream.ReadAsync(memory, cancellationToken);
                if (bytesRead != 0)
                {
                    _sendPipe.Writer.Advance(bytesRead);
                    await _sendPipe.Writer.FlushAsync(cancellationToken);
                }
            }

            return await WaitForRequestAsync(messageId, completion, cancellationToken);
        }

        private async ValueTask<ArraySegment<byte>> WaitForRequestAsync(Guid taskCompletionId, TaskCompletionSource<ArraySegment<byte>> source, CancellationToken cancellationToken)
        {
            var delayTask = Task.Delay(_options.DuplexTimeout, cancellationToken)
                .ContinueWith(_ =>
                {
                    if (source.Task.IsCompleted)
                        return;

                    source.TrySetException(new TimeoutException());

                    if (!_completions.TryRemove(taskCompletionId, out var __))
                    {
                        _logger?.LogError("{svrName}: Cannot remove task completion for MessageId = {msgId}", _appName, taskCompletionId);
                    }

                    if (_options.DisconnectOnTimeout)
                        Disconnect();
                }, cancellationToken);

            await Task.WhenAny(delayTask, source.Task);

            return source.Task.Result;
        }

        public async ValueTask ReplyAsync(Guid messageId, ArraySegment<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (!_options.Duplex)
                throw new NotSupportedException($"Cannot use ReplyAsync with {nameof(_options.Duplex)} option disabled");

            if (cancellationToken.IsCancellationRequested)
                return;

            using (await _mutex.LockAsync(cancellationToken))
            {
                _sendPipe.Writer.Write(BitConverter.GetBytes(buffer.Count + sizeof(int) + GUID_LEN));

                _sendPipe.Writer.Write(messageId.ToByteArray());

                var memory = _sendPipe.Writer.GetMemory(buffer.Count);
                buffer.AsMemory().CopyTo(memory);

                _sendPipe.Writer.Advance(buffer.Count);

                await _sendPipe.Writer.FlushAsync(cancellationToken);
            }
        }

        public async ValueTask ReplyAsync(Guid messageId, Stream stream, CancellationToken cancellationToken = default)
        {
            if (!_options.Duplex)
                throw new NotSupportedException($"Cannot use ReplyAsync with {nameof(_options.Duplex)} option disabled");

            if (cancellationToken.IsCancellationRequested)
                return;

            using (await _mutex.LockAsync(cancellationToken))
            {
                stream.Position = 0;

                _sendPipe.Writer.Write(BitConverter.GetBytes((int)stream.Length + sizeof(int) + GUID_LEN));

                _sendPipe.Writer.Write(messageId.ToByteArray());

                var memory = _sendPipe.Writer.GetMemory((int)stream.Length);
                var bytesRead = await stream.ReadAsync(memory, cancellationToken);
                if (bytesRead != 0)
                {
                    _sendPipe.Writer.Advance(bytesRead);
                    await _sendPipe.Writer.FlushAsync(cancellationToken);
                }
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

            _recvBuffer = ArrayPool<byte>.Shared.Rent(_options.RecvBufferSize + sizeof(int));
            try
            {
                _sendBuffer = ArrayPool<byte>.Shared.Rent(_options.SendBufferSize + sizeof(int));
                try
                {
                    var writing = FillPipeAsync(_cancellationTokenSource.Token);
                    var reading = ReadPipeAsync(_cancellationTokenSource.Token);
                    var sending = SendPipeAsync(_cancellationTokenSource.Token);

                    await Task.WhenAll(writing, reading, sending);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(_sendBuffer, true);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(_recvBuffer, true);
            }
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

        private async Task ReadPipeAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ReadResult result = await _receivePipe.Reader.ReadAsync(cancellationToken);
                    ReadOnlySequence<byte> buffer = result.Buffer;

                    while (!cancellationToken.IsCancellationRequested && await TryGetRecvMessage(ref buffer, out var message))
                    {
                        if (message.HasValue)
                        {
                            await OnReceivedMessageAsync(message.Value, cancellationToken);
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
            finally
            {
                Disconnect();
                await _receivePipe.Reader.CompleteAsync();
            }
        }

        private Task<bool> TryGetRecvMessage(ref ReadOnlySequence<byte> buffer, out NetXMessage? message)
        {
            message = null;

            const int DUPLEX_HEADER_SIZE = sizeof(int) + GUID_LEN;

            if (buffer.IsEmpty || (_options.Duplex && buffer.Length < DUPLEX_HEADER_SIZE))
            {
                return Task.FromResult(false);
            }

            var headerOffset = _options.Duplex ? DUPLEX_HEADER_SIZE : 0;

            var minRecvSize = Math.Min(_options.RecvBufferSize, buffer.Length);
            buffer.Slice(0, _options.Duplex ? headerOffset : minRecvSize).CopyTo(_recvBuffer);

            var size = _options.Duplex ? BitConverter.ToInt32(_recvBuffer) : GetReceiveMessageSize(new ArraySegment<byte>(_recvBuffer, 0, (int)minRecvSize));
            var messageId = _options.Duplex ? new Guid(new Span<byte>(_recvBuffer, sizeof(int), GUID_LEN)) : Guid.Empty;

            if (size > _options.RecvBufferSize)
                throw new Exception($"Recv Buffer is too small. RecvBuffLen = {_options.RecvBufferSize} ReceivedLen = {size}");

            if (size > buffer.Length)
                return Task.FromResult(false);

            buffer.Slice(headerOffset, size - headerOffset).CopyTo(_recvBuffer);

            var messageBuffer = new ArraySegment<byte>(_recvBuffer, 0, size - headerOffset);
            ProcessReceivedBuffer(in messageBuffer);

            var next = buffer.GetPosition(size);
            buffer = buffer.Slice(next);

            var resultBuffer = _options.CopyBuffer ? messageBuffer.ToArray() : messageBuffer;

            if (_options.Duplex && _completions.TryRemove(messageId, out var completion))
            {
                //If set result fails, it means that the message was received but the completion source was canceled or timed out. So we just log and ignore it.
                if (!completion.TrySetResult(resultBuffer))
                {
                    _logger?.LogError("{appName}: Failed to set duplex completion result. MessageId = {msgId}", _appName, messageId);
                    return completion.Task.ContinueWith(_ => true);
                }

                return Task.FromResult(true);
            }

            message = new NetXMessage(messageId, resultBuffer);
            return Task.FromResult(true);
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

                    while (!cancellationToken.IsCancellationRequested && TryGetSendMessage(ref buffer, out ArraySegment<byte> sendBuff))
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

        private bool TryGetSendMessage(ref ReadOnlySequence<byte> buffer, out ArraySegment<byte> sendBuff)
        {
            sendBuff = default;

            var offset = _options.Duplex ? 0 : sizeof(int);

            if (buffer.IsEmpty || buffer.Length < sizeof(int))
                return false;

            buffer.Slice(0, sizeof(int)).CopyTo(_sendBuffer);
            var size = BitConverter.ToInt32(_sendBuffer);

            if (size > _options.SendBufferSize)
                throw new Exception($"Send Buffer is too small. SendBuffLen = {_options.SendBufferSize} SendLen = {size}");

            if (size > buffer.Length)
                return false;

            buffer.Slice(offset, size).CopyTo(_sendBuffer);

            sendBuff = new ArraySegment<byte>(_sendBuffer, 0, size);

            ProcessSendBuffer(in sendBuff);

            var next = buffer.GetPosition(size + offset);
            buffer = buffer.Slice(next);

            return true;
        }

        protected virtual int GetReceiveMessageSize(in ArraySegment<byte> buffer)
        {
            return 0;
        }

        protected virtual void ProcessReceivedBuffer(in ArraySegment<byte> buffer)
        {
        }

        protected virtual void ProcessSendBuffer(in ArraySegment<byte> buffer)
        {
        }

        protected abstract Task OnReceivedMessageAsync(NetXMessage message, CancellationToken cancellationToken);
    }
}