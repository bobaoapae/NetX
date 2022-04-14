using System.Net;

namespace NetX.Options
{
    public class NetXServerOptions : NetXConnectionOptions
    {
        public INetXServerProcessor Processor { get; }
        public bool UseProxy { get; }
        public int Backlog { get; }

        public NetXServerOptions(
            INetXServerProcessor processor,
            IPEndPoint endPoint, 
            bool noDelay, 
            int recvBufferSize, 
            int sendBufferSize, 
            bool duplex,
            int duplexTimeout,
            bool copyBuffer,
            bool useProxy,
            int backLog,
            int socketTimeout,
            bool disconnectOnTimeout) : base(
                endPoint, 
                noDelay, 
                recvBufferSize, 
                sendBufferSize, 
                duplex,
                duplexTimeout,
                copyBuffer,
                socketTimeout,
                disconnectOnTimeout)
        {
            Processor = processor;
            UseProxy = useProxy;
            Backlog = backLog;
        }
    }
}
