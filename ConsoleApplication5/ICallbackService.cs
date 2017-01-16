using System;
using System.ServiceModel;
using System.Threading.Tasks;

namespace QueueProcessor
{
    [ServiceContract]
    public interface ICallbackService<in TRequest, TResponse> :
        IDisposable
    {
        [OperationContract]
        Task<TResponse> SendRequest(TRequest request);
    }
}