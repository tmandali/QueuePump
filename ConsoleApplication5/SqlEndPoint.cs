using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Xsl;

namespace QueueProcessor
{
    public class SqlEndPoint : EndPoint
    {
        private SqlConnectionFactory sqlConnectionFactory;
        private string procedureName;
        private Uri adress;
        private string xsltFile;
        
        protected override void Init(Uri adress, string xsltFile = null)
        {
            this.adress = adress;
            this.xsltFile = xsltFile ?? $"{adress.Segments[1]}.xslt";
            var connection = System.Configuration.ConfigurationManager.ConnectionStrings[adress.Host].ConnectionString;
            sqlConnectionFactory = SqlConnectionFactory.Default(connection);
            procedureName = adress.Segments[1];
        }
        
        public override async Task<bool> Send(Guid messageId, string from, XmlReader reader)
        {
            var xsltPath = Path.GetFullPath($@".\{adress.Host}\{from}\{xsltFile}");
            var exportFile = Path.GetFullPath($@".\{adress.Host}\{from}\Log\{messageId}.xml");

            var transformReader = Transform(exportFile, reader, xsltPath);

            using (var connection = await sqlConnectionFactory.OpenNewConnection().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var commmand = new SqlCommand(procedureName, connection, transaction);
                commmand.CommandType = CommandType.StoredProcedure;
                commmand.Parameters.AddWithValue("@MessageId", messageId);
                commmand.Parameters.AddWithValue("@From", from);
                commmand.Parameters.AddWithValue("@Xml", new SqlXml(transformReader));

                await commmand.ExecuteNonQueryAsync().ConfigureAwait(false);

                transaction.Commit();
                return true;
            }
        }
    }
}