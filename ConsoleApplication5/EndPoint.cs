using System;
using System.IO;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Xsl;

namespace QueueProcessor
{
    public abstract class EndPoint
    {
        public abstract Task<bool> Send(Guid messageId, string from, XmlReader reader);
        protected abstract void Init(Uri adress, string xsltFile = null);        

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

        protected XmlReader Transform(XmlReader input, string xsltFile)
        {
            if (File.Exists(xsltFile))
            {
                var ms = new MemoryStream();
                var outputWriter = XmlWriter.Create(ms);

                var xslt = new XslCompiledTransform();
                xslt.Load(xsltFile);
                xslt.Transform(input, outputWriter);
                return XmlReader.Create(ms);
            }

            return input;
        }
    }
}
