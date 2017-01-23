namespace QueueProcessor
{
    using System;
    using System.Threading.Tasks;

    public interface IReceiver
    {
        Task Init(Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError, TableBaseQueue InputQueue, string connection);        
        void Start(int? maxConcurrency = null);
        Task Stop();
    }
}