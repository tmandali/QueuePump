using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Threading.Tasks;
using System.Xml;

namespace QueueProcessor
{
    public class SqlEndPoint : EndPoint
    {
        private SqlConnectionFactory sqlConnectionFactory;
        private string procedureName;
        private string endPoint;

        protected override void Init(Uri adress)
        {
            endPoint = adress.ToString();
            var connection = System.Configuration.ConfigurationManager.ConnectionStrings[adress.Host].ConnectionString;
            sqlConnectionFactory = SqlConnectionFactory.Default(connection);
            procedureName = adress.Segments[1];
        }

        public override async Task<bool> Send(Guid messageId, string from, XmlReader reader)
        {
            using (var connection = await sqlConnectionFactory.OpenNewConnection().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var commmand = new SqlCommand(procedureName, connection, transaction);
                commmand.CommandType = CommandType.StoredProcedure;
                commmand.Parameters.AddWithValue("@MessageId", messageId);
                commmand.Parameters.AddWithValue("@From", from);
                commmand.Parameters.AddWithValue("@Xml", new SqlXml(reader));

                await commmand.ExecuteNonQueryAsync().ConfigureAwait(false);

                transaction.Commit();
                return true;
            }
        }
    }
}