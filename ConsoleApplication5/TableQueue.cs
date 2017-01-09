﻿using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Schema;

namespace QueueProcessor
{
    class TableQueue : IQueue
    {
        string tableName;
        SqlConnectionFactory connectionFactory;
        XmlReader schema;

        public TableQueue(string tableName, string connection, XmlReader schema)
        {
            this.tableName = tableName;
            this.connectionFactory = SqlConnectionFactory.Default(connection);
            this.schema = schema;
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

                if (schema != null)
                {
                    XmlReaderSettings settings = new XmlReaderSettings();
                    settings.ConformanceLevel = ConformanceLevel.Auto;
                    settings.ValidationType = ValidationType.Schema;
                    settings.ValidationFlags |= XmlSchemaValidationFlags.ProcessInlineSchema;
                    settings.ValidationFlags |= XmlSchemaValidationFlags.ProcessSchemaLocation;
                    settings.ValidationFlags |= XmlSchemaValidationFlags.ReportValidationWarnings;
                    settings.ValidationEventHandler += new ValidationEventHandler(ValidationCallBack);
                    settings.Schemas.Add("http://www.lcwaikiki.com/queue." + tableName, schema);
                    xmlReader = XmlReader.Create(xmlReader, settings);
                }

                envelope.MessageId = (Guid) command.Parameters["@MessageId"].Value;

                var endPoint = await EndPoint.Factory(envelope);
                result = await endPoint.Send(envelope.MessageId, tableName, xmlReader);

                transaction.Commit();
            }

            return result;
        }

        private void ValidationCallBack(object sender, ValidationEventArgs args)
        {
            if (args.Severity == XmlSeverityType.Warning)
                Trace.TraceWarning(args.Message);
            else         
                throw new XmlSchemaValidationException(args.Message, args.Exception);
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
