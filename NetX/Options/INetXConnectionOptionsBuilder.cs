using System.Net;

namespace NetX.Options
{
    public interface INetXConnectionOptionsBuilder<T>
    {
        INetXConnectionOptionsBuilder<T> EndPoint(IPEndPoint endPoint);
        INetXConnectionOptionsBuilder<T> EndPoint(string address, ushort port);
        INetXConnectionOptionsBuilder<T> NoDelay(bool noDelay);
        INetXConnectionOptionsBuilder<T> DuplexTimeout(int timeoutMs);
        INetXConnectionOptionsBuilder<T> ReceiveBufferSize(int size);
        INetXConnectionOptionsBuilder<T> SendBufferSize(int size);
        INetXConnectionOptionsBuilder<T> MaxFrameBytes(int size);
        INetXConnectionOptionsBuilder<T> SocketTimeout(int timeout);
        INetXConnectionOptionsBuilder<T> DisconnectOnTimeout(bool disconnectOnTimeout);
        INetXConnectionOptionsBuilder<T> ReuseSocket(bool reuseSocket);
        T Build();
    }
}
