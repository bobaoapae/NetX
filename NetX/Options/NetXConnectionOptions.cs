using System.Net;

namespace NetX.Options
{
    public class NetXConnectionOptions
    {
        public IPEndPoint EndPoint { get; }
        public bool NoDelay { get; }
        public int RecvBufferSize { get; }
        public int SendBufferSize { get; }
        public bool Duplex { get; }
        public int DuplexTimeout { get; }
        public bool CopyBuffer { get; }
        public int SocketTimeout { get; }
        public bool DisconnectOnTimeout { get; }

        public NetXConnectionOptions(
            IPEndPoint endPoint,
            bool noDelay,
            int recvBufferSize,
            int sendBufferSize,
            bool duplex,
            int duplexTimeout,
            bool copyBuffer,
            int socketTimeout,
            bool disconnectOnTimeout)
        {
            EndPoint = endPoint;
            NoDelay = noDelay;
            RecvBufferSize = recvBufferSize;
            SendBufferSize = sendBufferSize;
            Duplex = duplex;
            DuplexTimeout = duplexTimeout;
            CopyBuffer = copyBuffer;
            SocketTimeout = socketTimeout;
            DisconnectOnTimeout = disconnectOnTimeout;
        }
    }
}