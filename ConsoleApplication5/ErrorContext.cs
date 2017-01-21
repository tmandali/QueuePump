using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace QueueProcessor
{
    public class ErrorContext
    {        
        public ErrorContext(Exception exception, Message message, TransactionScope transportTransaction, int processingAttempts)
        {
            Exception = exception;
            Message = message;
            TransportTransaction = transportTransaction;
            DelayedDeliveriesPerformed = processingAttempts;
        }

        public int DelayedDeliveriesPerformed { get; }
        public Exception Exception { get; }
        public Message Message { get; }        
        public TransactionScope TransportTransaction { get; }
    }
}
