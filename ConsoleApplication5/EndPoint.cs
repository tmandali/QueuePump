using System;
using System.Threading.Tasks;
using System.Xml;

namespace QueueProcessor
{
    public abstract class EndPoint
    {
        public abstract Task<bool> Send(string from, XmlReader reader);
        protected abstract void Init(Uri adress);
        public static Task<EndPoint> Factory(Envelope envelope)
        {
            EndPoint result;

            switch (envelope.EndPoint.Scheme)
            {
                case "mssql":
                    result = new SqlEndPoint();
                    break;
                default:
                    throw new Exception($"{envelope.EndPoint.Scheme} desteklenmityor !");
            }

            result.Init(envelope.EndPoint);
            return Task.FromResult(result);
        }
    }

}
