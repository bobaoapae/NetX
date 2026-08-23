using System.Net;

namespace NetX.Options
{
    public abstract class NetXConnectionOptionsBuilder<T> : INetXConnectionOptionsBuilder<T>
    {
        // 16 MiB default — decoupled from RecvBufferSize/SendBufferSize (those size the socket-level
        // scratch buffers; this bounds how large a single logical frame is allowed to accumulate to).
        internal const int DefaultMaxFrameBytes = 16 * 1024 * 1024;

        protected IPEndPoint _endpoint;
        protected bool _noDelay = false;
        protected int _recvBufferSize = 1024;
        protected int _sendBufferSize = 1024;
        protected int _duplexTimeout = 5000;
        protected int _maxFrameBytes = DefaultMaxFrameBytes;
        protected int _socketTimeout = 0;
        protected bool _disconnectOnTimeout = true;
        protected bool _reuseSocket = true;

        public INetXConnectionOptionsBuilder<T> EndPoint(IPEndPoint endPoint)
        {
            _endpoint = endPoint;
            return this;
        }

        public INetXConnectionOptionsBuilder<T> EndPoint(string address, ushort port)
            => EndPoint(new IPEndPoint(IPAddress.Parse(address), port));

        public INetXConnectionOptionsBuilder<T> NoDelay(bool noDelay)
        {
            _noDelay = noDelay;
            return this;
        }

        public INetXConnectionOptionsBuilder<T> DuplexTimeout(int timeoutMs)
        {
            _duplexTimeout = timeoutMs;
            return this;
        }

        public INetXConnectionOptionsBuilder<T> ReceiveBufferSize(int size)
        {
            _recvBufferSize = size;
            return this;
        }

        public INetXConnectionOptionsBuilder<T> SendBufferSize(int size)
        {
            _sendBufferSize = size;
            return this;
        }

        public INetXConnectionOptionsBuilder<T> MaxFrameBytes(int size)
        {
            _maxFrameBytes = size;
            return this;
        }

        public INetXConnectionOptionsBuilder<T> SocketTimeout(int timeout)
        {
            _socketTimeout = timeout;
            return this;
        }

        public INetXConnectionOptionsBuilder<T> DisconnectOnTimeout(bool disconnectOnTimeout)
        {
            _disconnectOnTimeout = disconnectOnTimeout;
            return this;
        }

        public INetXConnectionOptionsBuilder<T> ReuseSocket(bool reuseSocket)
        {
            _reuseSocket = reuseSocket;
            return this;
        }

        public abstract T Build();
    }
}
