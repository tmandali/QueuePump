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

        protected async Task<XmlReader> Transform(Guid messageId, string host, string from, string xsltFile, XmlReader input)
        {
            var xsltPath = Path.GetFullPath($@".\{host}\{from}\{xsltFile}");
            var exportFile = Path.GetFullPath($@".\{host}\{from}\Log\{messageId}.xml");
                        
            Trace.TraceInformation($"Endpoint xml file : {exportFile}");
            return await Transform(exportFile, xsltPath, input).ConfigureAwait(false);
        }

        protected async Task<XmlReader> Transform(string exportFile, string xsltFile, XmlReader input)
        {
            var directory = Path.GetDirectoryName(exportFile);
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);


            var outputWriterSettings = new XmlWriterSettings() { Async = true };

            using (var outputWriter = XmlWriter.Create(exportFile, outputWriterSettings))
            {
                if (File.Exists(xsltFile))
                {
                    var xslt = new XslCompiledTransform();
                    xslt.Load(xsltFile);
                    xslt.Transform(input, outputWriter);
                    Trace.TraceInformation($"Transform xml file : {xsltFile}");
                }
                else
                {
                    await outputWriter.WriteNodeAsync(input, false).ConfigureAwait(false);
                }

                outputWriter.Close();
                return XmlReader.Create(exportFile);
            }
        }
    }
}