using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueueProcessor
{
    public class Message
    {
        public Message(Guid messageId, dynamic body)
        {
            MessageId = messageId;
            Body = body;
        }
        
        public dynamic Body { get; }
        public Guid MessageId { get; }
    }
}
