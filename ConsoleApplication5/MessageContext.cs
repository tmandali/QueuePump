namespace QueueProcessor
{
    using System.Threading;
    using System.Transactions;

    public class MessageContext
    {        
        public MessageContext(Message message, TransactionScope transportTransaction, CancellationTokenSource receiveCancellationTokenSource)
        {
            ReceiveCancellationTokenSource = receiveCancellationTokenSource;
            TransportTransaction = transportTransaction;
            Message = message;
        }

        public Message Message { get; }
        public CancellationTokenSource ReceiveCancellationTokenSource { get; }
        public TransactionScope TransportTransaction { get; }
    }
}