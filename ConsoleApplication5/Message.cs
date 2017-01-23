namespace QueueProcessor
{
    using System;

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
