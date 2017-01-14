using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace QueueProcessor
{
    class HostConnect
    {
        ConnectionStringSettings host;
        SqlConnectionFactory sqlConnectionFactory;
        SemaphoreSlim concurrencyLimiter;

        public HostConnect(ConnectionStringSettings host)
        {
            this.host = host;
        }

        public async Task Starter(TimeSpan retry, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    concurrencyLimiter = new SemaphoreSlim(2);
                    sqlConnectionFactory = SqlConnectionFactory.Default(host.ConnectionString);

                    await Processor(cancellationToken).ConfigureAwait(false);                    
                    await Task.Delay(retry, cancellationToken).ConfigureAwait(false);

                    Trace.TraceInformation($"{host.Name} retry {retry}");
                }
                catch (OperationCanceledException)
                {
                    // For graceful shutdown purposes
                }
                catch (SqlException e) when (cancellationToken.IsCancellationRequested)
                {
                    Trace.TraceError("Exception thrown during cancellation", e);
                }
                catch (SqlException e) when (e.ErrorCode == - 2146232060)
                {
                    //Trace.TraceError($"Sql connection failed, retry {retry}");
                    await Task.Delay(retry);
                }
                catch (Exception ex)
                {
                    Trace.TraceError($"Sql connector failed", ex);
                }
                finally
                {
                    concurrencyLimiter.Dispose();
                }
            }
        }

        async Task Processor(CancellationToken cancellationToken)
        {
            using (var connection = await sqlConnectionFactory.OpenNewConnection().ConfigureAwait(false))
            {
                
                var command = new SqlCommand("SELECT [Table], [MaxConcurrency] FROM [Queue]", connection);
                var dataReader = await command.ExecuteReaderAsync().ConfigureAwait(false);
                while (await TryReadQueue(dataReader, cancellationToken).ConfigureAwait(false) && !cancellationToken.IsCancellationRequested) {                    
                };                
            }
        }

        async Task<bool> TryReadQueue(SqlDataReader dataReader, CancellationToken cancellationToken)
        {
            await concurrencyLimiter.WaitAsync().ConfigureAwait(false);

            if (!await dataReader.ReadAsync().ConfigureAwait(false))
                return false;

            dynamic queueDefination = new ExpandoObject();
            queueDefination.TableName = await dataReader.GetFieldValueAsync<string>(0).ConfigureAwait(false);
            var maxConcurrency = await dataReader.GetFieldValueAsync<int>(1).ConfigureAwait(false);
            var queue = new TableQueue(host.Name, queueDefination.TableName, host.ConnectionString).Receive(cancellationToken).ContinueWith(c=> concurrencyLimiter.Release()).ConfigureAwait(false);

            return true;
        }
    }
}