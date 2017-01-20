using System.Data;
using System.Threading;
using System.Transactions;

namespace QueueProcessor
{
    public class MessageContext
    {        
        public MessageContext(TransactionScope transportTransaction)
        {
            TransportTransaction = transportTransaction;
        }

        public Message Body { get; }
        public CancellationTokenSource ReceiveCancellationTokenSource { get; }
        public TransactionScope TransportTransaction { get; }
    }
}