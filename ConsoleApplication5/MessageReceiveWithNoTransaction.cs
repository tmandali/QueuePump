using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace QueueProcessor
{
    class MessageReceiveWithNoTransaction : MessageReceiver
    {
        SqlConnectionFactory connectionFactory;

        public MessageReceiveWithNoTransaction(SqlConnectionFactory connectionFactory)
        {
            this.connectionFactory = connectionFactory;
        }

        public override async Task ReceiveMessage(CancellationTokenSource receiveCancellationTokenSource)
        {
            throw new NotImplementedException();
        }
    }
}
