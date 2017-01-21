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
        public Message(Guid messageId, ExpandoObject body)
        {
            MessageId = messageId;
            Body = body;
        }
        
        public ExpandoObject Body { get; }
        public Guid MessageId { get; }
    }
}
