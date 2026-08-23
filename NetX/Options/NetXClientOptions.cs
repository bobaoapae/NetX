using System.Net;

namespace NetX.Options
{
    internal class NetXClientOptions : NetXConnectionOptions
    {
        public INetXClientProcessor Processor { get; }

        public NetXClientOptions(
            INetXClientProcessor processor,
            IPEndPoint endPoint,
            bool noDelay,
            int recvBufferSize,
            int sendBufferSize,
            int duplexTimeout,
            int maxFrameBytes,
            int socketTimeout,
            bool disconnectOnTimeout,
            bool reuseSocket) : base(
                endPoint,
                noDelay,
                recvBufferSize,
                sendBufferSize,
                duplexTimeout,
                maxFrameBytes,
                socketTimeout,
                disconnectOnTimeout,
                reuseSocket)
        {
            Processor = processor;
        }
    }
}
