namespace QueueProcessor
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Dynamic;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Linq;
    using static System.String;

    public class TableBasedQueue
    {
        string tableName;
        string schemaName;

        public string[] Keys { get; } 

        public override string ToString()
        {
            return $"{schemaName}.{tableName}";
        }

        public TableBasedQueue(string schemaName, string tableName, IEnumerable<string> keys)
        {            
            using (var sanitizer = new SqlCommandBuilder())
            {
                this.tableName = sanitizer.QuoteIdentifier(tableName);
                this.schemaName = sanitizer.QuoteIdentifier(schemaName);
            }

            this.Keys = keys.ToArray(); 
        }

        public virtual async Task<int> TryPeek(SqlConnection connection, CancellationToken token, int timeoutInSeconds = 30)
        {
            var peekText = $"SELECT count(*) Id FROM {schemaName}.{tableName} WITH (READPAST)";

            using (var command = new SqlCommand(peekText, connection) { CommandTimeout = timeoutInSeconds})
            {
                var numberOfMessages = (int)await command.ExecuteScalarAsync(token).ConfigureAwait(false);

                return numberOfMessages;
            }
        }

        public virtual async Task<Message> TryReceive(SqlConnection connection, SqlTransaction transaction, CancellationTokenSource receiveCancellationTokenSource)
        {
            string receiveText = $@"
            DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
            IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON';
            SET NOCOUNT ON;
            
            WITH message AS (SELECT TOP(1) * FROM {schemaName}.{tableName} WITH (UPDLOCK, READPAST, ROWLOCK) ORDER BY [RowVersion]) 
            DELETE FROM message
            OUTPUT deleted.*;
            IF(@NOCOUNT = 'ON') SET NOCOUNT ON;
            IF(@NOCOUNT = 'OFF') SET NOCOUNT OFF;";

            using (var command = new SqlCommand(receiveText, connection, transaction))
            {
                return await ReadMessage(this.Keys, command).ConfigureAwait(false);                
            }
        }

        static Guid ConvertToMd5HashGUID(string value)
        {
            // convert null to empty string - null can not be hashed
            if (value == null)
                value = string.Empty;

            // get the byte representation
            var bytes = Encoding.Default.GetBytes(value);

            // create the md5 hash
            var md5Hasher = MD5.Create();
            byte[] data = md5Hasher.ComputeHash(bytes);

            // convert the hash to a Guid
            return new Guid(data);
        }

        static async Task<Message> ReadMessage(string[] keys, SqlCommand command)
        {
            // We need sequential access to not buffer everything into memory
            using (var dataReader = await command.ExecuteReaderAsync(System.Data.CommandBehavior.SingleRow | System.Data.CommandBehavior.SequentialAccess).ConfigureAwait(false))
            {
                if (!await dataReader.ReadAsync().ConfigureAwait(false))
                {
                    return null;
                }

                var readResult = await ReadRow(dataReader).ConfigureAwait(false);
                var messageId = ConvertToMd5HashGUID(Join("@", readResult.Where(w => keys.Contains(w.Key)).Select(s => s.Value)));

                return new Message(messageId, readResult);         
            }
        }

        static async Task<ExpandoObject> ReadRow(SqlDataReader dataReader)
        {
            var body = new ExpandoObject();

            var properyBag = (ICollection<KeyValuePair<string, object>>) body;
            for (int i = 0; i < dataReader.FieldCount; i++)
            {
                string name = dataReader.GetName(i);
                properyBag.Add(new KeyValuePair<string, object>(name, await GetNullableAsync<object>(dataReader, i).ConfigureAwait(false)));
            }

            return body;
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


//public virtual async Task<string[]> TryConstraint(SqlConnection connection, CancellationToken token, string constraintType = "PRIMARY KEY")
//{
//    var keyText = $@"SELECT COLUMN_NAME
//                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
//                    JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU ON TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME
//                    WHERE TC.CONSTRAINT_TYPE = '{constraintType}' AND CCU.TABLE_NAME = '{tableName}' AND TABLE_SCHEMA = '{schemaName}' ";            

//    using (var command = new SqlCommand(keyText, connection))
//    {
//        var fields = new List<string>();
//        var dataReader = await command.ExecuteReaderAsync().ConfigureAwait(false);

//        while (await dataReader.ReadAsync().ConfigureAwait(false))
//        {
//            fields.Add(await ConstraintRow(dataReader, token));
//        }

//        return fields.ToArray();
//    }
//}

//static async Task<string> ConstraintRow(SqlDataReader dataReader, CancellationToken token)
//{
//    return await dataReader.GetFieldValueAsync<string>(0, token).ConfigureAwait(false);
//}