using System.Threading;
using System.Threading.Tasks;

namespace NetX
{
    public interface INetXClient : INetXConnection
    {
        Task ConnectAsync(CancellationToken cancellationToken = default);
    }
}
