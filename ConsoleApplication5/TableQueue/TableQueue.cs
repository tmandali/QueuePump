using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Schema;
using System.Linq;

namespace QueueProcessor
{
    class TableQueue : IQueue
    {
        string tableName;

        public string Name { get; }

        SqlConnectionFactory connectionFactory;

        public TableQueue(string name, string tableName, string connection)
        {
            this.Name = name;
            this.tableName = tableName;
            this.connectionFactory = SqlConnectionFactory.Default(connection);
        }

        public async Task Receive(CancellationToken ct)
        {
            Trace.TraceInformation($"{Name}:{tableName} started");
            await Task.Delay(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            
            while (!ct.IsCancellationRequested)
            {
                //var stopwatch = Stopwatch.StartNew();                
                Envelope envelope = null;

                using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
                using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    envelope = await TryReceive(connection, transaction, ct).ConfigureAwait(false);
                  
                    if (envelope == null)
                    {
                        Trace.TraceInformation($"{Name}:{tableName} Not received message");
                        transaction.Commit();
                        return;
                    }

                    Trace.TraceInformation($"{Name}:{tableName} Received message");
                    
                    try
                    {
                        await TryProcess(envelope).ConfigureAwait(false);
                        Trace.TraceInformation($"{Name}:{tableName} Processed message");
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceError($"{Name}:{tableName} Error : {ex.Message}");
                        await Rollback(connection, transaction, ct, envelope, ex).ConfigureAwait(false);                        
                    }

                    transaction.Commit();
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

                var xsdPath = Path.GetFullPath($@".\{Name}\{tableName}\schema.xsd");
                if (File.Exists(xsdPath))
                {
                    var settings = new XmlReaderSettings();
                    settings.Schemas.Add(null, xsdPath);
                    settings.ConformanceLevel = ConformanceLevel.Auto;
                    settings.ValidationType = ValidationType.Schema;
                    settings.ValidationFlags |= XmlSchemaValidationFlags.ProcessSchemaLocation;
                    settings.ValidationFlags |= XmlSchemaValidationFlags.ReportValidationWarnings;
                    settings.ValidationFlags |= XmlSchemaValidationFlags.ProcessIdentityConstraints;
                    settings.ValidationEventHandler += Settings_ValidationEventHandler;

                    xmlReader = XmlReader.Create(xmlReader, settings);
                    Trace.TraceInformation($"Schema Validation {xsdPath}");
                }

                var endPoint = await EndPoint.Factory(envelope);
                result = await endPoint.Send(envelope.MessageId, tableName, xmlReader);

                transaction.Commit();
            }
            return result;
        }

        private void Settings_ValidationEventHandler(object sender, ValidationEventArgs e)
        {
             if (e.Severity == XmlSeverityType.Warning)
                Trace.TraceWarning(e.Message);
            else
                throw new XmlSchemaValidationException(e.Message, e.Exception);
        }

        public virtual async Task<Envelope> TryReceive(SqlConnection connection, SqlTransaction transaction, CancellationToken ct)
        {
            string receiveText = $@"
            DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
            IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON';
            SET NOCOUNT ON;
            
            WITH message AS (SELECT TOP(1) * FROM {tableName} WITH (UPDLOCK, READPAST, ROWLOCK) ORDER BY [RowVersion]) 
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

        async Task Rollback(SqlConnection connection, SqlTransaction transaction, CancellationToken ct, Envelope envelope, Exception ex)
        {
            //var rec = (ICollection<KeyValuePair<string, object>>)envelope.Headers;
            //rec.Add(new KeyValuePair<string, object>("ReplyTo", envelope.ReplyTo.ToString()));
            //rec.Add(new KeyValuePair<string, object>("EndPoint", envelope.EndPoint.ToString()));
            //rec.Add(new KeyValuePair<string, object>("DeliveryDate", envelope.DeliveryDate.AddHours(1)));
            //rec.Add(new KeyValuePair<string, object>("Error",  $"{ex.GetType()}:\r{ex.Message}" ));
            //rec.Add(new KeyValuePair<string, object>("RowVersion", envelope.RowVersion));                        

            //var fields = string.Join(",", rec.Select(r => r.Key));
            //var fieldPrms = string.Join(",", rec.Select(r => "@" + r.Key));
            //var cmdPrms = rec.Select(r => new SqlParameter("@" + r.Key, r.Value)).ToArray();

            //string rollbackText = $@"insert into {tableName} ({fields}) values ({fieldPrms})";

            //using (var command = new SqlCommand(rollbackText, connection, transaction))
            //{
            //    command.Parameters.AddRange(cmdPrms);
            //    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            //}
        }
    }
}
