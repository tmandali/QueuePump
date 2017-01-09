using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace QueueProcessor
{
    class TableQueue : IQueue
    {
        private string tableName;
        private SqlConnectionFactory connectionFactory;

        public TableQueue(string connection, string tableName)
        {
            this.tableName = tableName;
            this.connectionFactory = SqlConnectionFactory.Default(connection);
        }

        public async Task Receive(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                var stopwatch = Stopwatch.StartNew();
                
                Envelope envelope = null;

                using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
                using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    envelope = await TryReceive(connection, transaction, ct).ConfigureAwait(false);

                    stopwatch.Stop();
                  
                    if (envelope == null)
                    {
                        Trace.TraceInformation($"Not received message {stopwatch.Elapsed}");
                        transaction.Commit();
                        return;
                    }

                    Trace.TraceInformation($"Received message {stopwatch.Elapsed}");

                    stopwatch.Start();

                    if (await TryProcess(envelope).ConfigureAwait(false))
                    {
                        stopwatch.Stop();
                        Trace.TraceInformation($"Processed message {stopwatch.Elapsed}");
                        transaction.Commit();
                    }
                    else
                    {
                        Trace.TraceInformation($"Rollback message");
                        transaction.Rollback();
                    }
                }
            }
        }

        async Task<bool> TryProcess(Envelope envelope)
        {
            bool result = false;

            using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
            using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var command = new SqlCommand(envelope.ReplyTo, connection, transaction);
                envelope.PrepareExportCommand(command);
                var xmlReader = await command.ExecuteXmlReaderAsync().ConfigureAwait(false);

                envelope.MessageId = (Guid) command.Parameters["@MessageId"].Value;

                var endPoint = await EndPoint.Factory(envelope);
                result = await endPoint.Send(envelope.MessageId, tableName, xmlReader);

                transaction.Commit();
            }

            return result;
        }

        async Task<Envelope> TryReceive(SqlConnection connection, SqlTransaction transaction, CancellationToken ct)
        {
            string receiveText = $@"
            DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
            IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON';
            SET NOCOUNT ON;

            --OUTPUT deleted.Id, deleted.CorrelationId, deleted.ReplyToAddress, deleted.Recoverable, deleted.Headers, deleted.Body;

            WITH message AS (SELECT TOP(1) * FROM {tableName} WITH (UPDLOCK, READPAST, ROWLOCK)) -- WHERE [Expires] IS NULL OR [Expires] > GETUTCDATE() ORDER BY [RowVersion]
            DELETE FROM message
            OUTPUT deleted.*;
            IF(@NOCOUNT = 'ON') SET NOCOUNT ON;
            IF(@NOCOUNT = 'OFF') SET NOCOUNT OFF;";

            using (var command = new SqlCommand(receiveText, connection, transaction))
            {
                return await ReadMessage(command).ConfigureAwait(false);
            }
        }

        async Task<Envelope> ReadMessage(SqlCommand command)
        {
            using (var dataReader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow | CommandBehavior.SequentialAccess).ConfigureAwait(false))
            {
                if (!await dataReader.ReadAsync().ConfigureAwait(false))
                {
                    return null;
                }

                return await Envelope.Read(dataReader).ConfigureAwait(false);
            }
        }
    }
}
