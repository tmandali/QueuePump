using System.Threading;
using System.Threading.Tasks;

namespace QueueProcessor
{
    internal interface IQueue
    {
        Task Receive(CancellationToken ct);
    }
}
