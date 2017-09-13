using System.Threading;
using System.Threading.Tasks;

namespace QueueProcessor
{
    abstract class MessageReceiver
    {
        public abstract Task ReceiveMessage(CancellationTokenSource receiveCancellationTokenSource);
    }
}