namespace QueueProcessor
{
    using System;
    using System.Transactions;

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
