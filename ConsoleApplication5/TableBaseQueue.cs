namespace QueueProcessor
{
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Dynamic;
    using System.Threading;
    using System.Threading.Tasks;

    public class TableBaseQueue
    {
        string tableName;
        string schemaName;

        public TableBaseQueue(string schemaName, string tableName)
        {
            using (var sanitizer = new SqlCommandBuilder())
            {
                this.tableName = sanitizer.QuoteIdentifier(tableName);
                this.schemaName = sanitizer.QuoteIdentifier(schemaName);
            }
        }

        public virtual async Task<int> TryPeek(SqlConnection connection, CancellationToken token, int timeoutInSeconds = 30)
        {
            var peekText = $"SELECT count(*) Id FROM {schemaName}.{tableName} WITH (READPAST)";

            using (var command = new SqlCommand(peekText, connection)
            {
                CommandTimeout = timeoutInSeconds
            })
            {
                var numberOfMessages = (int)await command.ExecuteScalarAsync(token).ConfigureAwait(false);

                return numberOfMessages;
            }
        }

        public virtual async Task<ExpandoObject> TryReceive(SqlConnection connection, SqlTransaction transaction, CancellationTokenSource receiveCancellationTokenSource)
        {
            string receiveText = $@"
            DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
            IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON';
            SET NOCOUNT ON;
            
            WITH message AS (SELECT TOP(1) * FROM {schemaName}.{tableName} WITH (UPDLOCK, READPAST, ROWLOCK) WHERE [DeliveryDate] <= GETUTCDATE() ORDER BY [RowVersion]) 
            DELETE FROM message
            OUTPUT deleted.*;
            IF(@NOCOUNT = 'ON') SET NOCOUNT ON;
            IF(@NOCOUNT = 'OFF') SET NOCOUNT OFF;";

            using (var command = new SqlCommand(receiveText, connection, transaction))
            {
                return await ReadMessage(command).ConfigureAwait(false);
            }
        }

        static async Task<ExpandoObject> ReadMessage(SqlCommand command)
        {
            // We need sequential access to not buffer everything into memory
            using (var dataReader = await command.ExecuteReaderAsync(System.Data.CommandBehavior.SingleRow | System.Data.CommandBehavior.SequentialAccess).ConfigureAwait(false))
            {
                if (!await dataReader.ReadAsync().ConfigureAwait(false))
                {
                    return null;
                }

                var readResult = await ReadRow(dataReader).ConfigureAwait(false);

                return readResult;
            }
        }

        static async Task<ExpandoObject> ReadRow(SqlDataReader dataReader)
        {
            var result = new ExpandoObject();

            var properyBag = (ICollection<KeyValuePair<string, object>>)result;
            for (int i = 0; i < dataReader.FieldCount; i++)
            {
                string name = dataReader.GetName(i);
                properyBag.Add(new KeyValuePair<string, object>(name, await GetNullableAsync<object>(dataReader, i).ConfigureAwait(false)));
            }

            return result;
        }

        static async Task<T> GetNullableAsync<T>(SqlDataReader dataReader, int index)
        {
            if (await dataReader.IsDBNullAsync(index).ConfigureAwait(false))
            {
                return default(T);
            }
            return await dataReader.GetFieldValueAsync<T>(index).ConfigureAwait(false);
        }

    }
}
