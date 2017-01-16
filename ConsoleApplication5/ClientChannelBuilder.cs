using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;

namespace QueueProcessor
{
    public static class ClientChannelBuilder
    {
        public static ChannelFactory<ICallbackService<TMessage, TResponse>> GetChannelFactory<TMessage, TResponse>(string server)
        {
            var myBinding = new BasicHttpBinding();
            var address = AddressBuilder.GetAddress<TMessage, TResponse>(server);
            var myEndpoint = new EndpointAddress(address);
            return new ChannelFactory<ICallbackService<TMessage, TResponse>>(myBinding, myEndpoint);
        }
    }
}
