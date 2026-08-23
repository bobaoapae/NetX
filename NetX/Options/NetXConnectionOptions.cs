using System.Net;

namespace NetX.Options
{
    public class NetXConnectionOptions
    {
        public IPEndPoint EndPoint { get; }
        public bool NoDelay { get; }
        public int RecvBufferSize { get; }
        public int SendBufferSize { get; }
        public int DuplexTimeout { get; }
        public int MaxFrameBytes { get; }
        public int SocketTimeout { get; }
        public bool ReuseSocket { get; }
        public bool DisconnectOnTimeout { get; }

        public NetXConnectionOptions(
            IPEndPoint endPoint,
            bool noDelay,
            int recvBufferSize,
            int sendBufferSize,
            int duplexTimeout,
            int maxFrameBytes,
            int socketTimeout,
            bool disconnectOnTimeout,
            bool reuseSocket)
        {
            EndPoint = endPoint;
            NoDelay = noDelay;
            RecvBufferSize = recvBufferSize;
            SendBufferSize = sendBufferSize;
            DuplexTimeout = duplexTimeout;
            MaxFrameBytes = maxFrameBytes;
            SocketTimeout = socketTimeout;
            DisconnectOnTimeout = disconnectOnTimeout;
            ReuseSocket = reuseSocket;
        }
    }
}
