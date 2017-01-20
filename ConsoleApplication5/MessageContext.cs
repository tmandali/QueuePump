using System.Collections.Generic;
using System.Dynamic;
using System.Threading;
using System.Transactions;
using System.Linq;

namespace QueueProcessor
{
    public class MessageContext
    {        
        public MessageContext(ExpandoObject body, TransactionScope transportTransaction, CancellationTokenSource receiveCancellationTokenSource)
        {
            ReceiveCancellationTokenSource = receiveCancellationTokenSource;
            TransportTransaction = transportTransaction;
            Body = body;
        }

        public ExpandoObject Body { get; }
        public CancellationTokenSource ReceiveCancellationTokenSource { get; }
        public TransactionScope TransportTransaction { get; }
    }
}