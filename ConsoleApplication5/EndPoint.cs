using System;
using System.Diagnostics;
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
                    throw new Exception($"{envelope.EndPoint.Scheme} not supported !");
            }

            result.Init(envelope.EndPoint);
            return Task.FromResult(result);
        }

        protected static XmlReader Transform(Guid messageId, string host, string from, string xsltFile, XmlReader input)
        {
            var xsltPath = Path.GetFullPath($@".\{host}\{from}\{xsltFile}");
            var exportFile = Path.GetFullPath($@".\{host}\{from}\Log\{messageId}.xml");
            return Transform(exportFile, xsltFile, input);
        }

        protected static XmlReader Transform(string exportFile, string xsltFile, XmlReader input)
        {
            var directory = Path.GetDirectoryName(exportFile);
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);

            using (var outputWriter = XmlWriter.Create(exportFile))
            {
                if (File.Exists(xsltFile))
                {
                    var xslt = new XslCompiledTransform();
                    xslt.Load(xsltFile);
                    xslt.Transform(input, outputWriter);
                    Trace.TraceInformation($"Transform xml {xsltFile}");
                }
                else
                {
                    outputWriter.WriteNode(input, false);
                }

                outputWriter.Close();
                Trace.TraceInformation($"Endpoint xml file {exportFile}");
                return XmlReader.Create(exportFile);
            }
        }
    }
}