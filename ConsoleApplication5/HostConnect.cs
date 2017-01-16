using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace QueueProcessor
{
    class HostConnect
    {
        ConnectionStringSettings host;
        SqlConnectionFactory sqlConnectionFactory;
        SemaphoreSlim concurrencyLimiter;
        ConcurrentDictionary<Task, Task> runningReceiveTasks;

        public HostConnect(ConnectionStringSettings host)
        {
            this.host = host;
        }

        public async Task Starter(int maxConcurrency, TimeSpan retry, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    concurrencyLimiter = new SemaphoreSlim(maxConcurrency);
                    runningReceiveTasks = new ConcurrentDictionary<Task, Task>();
                    sqlConnectionFactory = SqlConnectionFactory.Default(host.ConnectionString);

                    await Processor(cancellationToken).ConfigureAwait(false);
                    await Task.WhenAll(runningReceiveTasks.Values).ConfigureAwait(false);

                    Trace.TraceInformation($"{host.Name} wait {retry}");
                    await Task.Delay(retry, cancellationToken).ConfigureAwait(false);
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
                    await Task.Delay(retry, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Trace.TraceError($"Sql connector failed => {ex.Message}", ex);
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
                var command = new SqlCommand("SELECT [Table] FROM [Queue]", connection);
                var dataReader = await command.ExecuteReaderAsync().ConfigureAwait(false);
                while (await TryReadQueue(dataReader, cancellationToken).ConfigureAwait(false) && !cancellationToken.IsCancellationRequested) {};                
            }
        }

        ConcurrentBag<TableQueue> cb = new ConcurrentBag<TableQueue>();

        async Task<bool> TryReadQueue(SqlDataReader dataReader, CancellationToken cancellationToken)
        {
            await concurrencyLimiter.WaitAsync(cancellationToken).ConfigureAwait(false);

            if (!await dataReader.ReadAsync().ConfigureAwait(false))
                return false;

            var tableName = await dataReader.GetFieldValueAsync<string>(0).ConfigureAwait(false);

            var receive = Receiver(tableName, cancellationToken);
            runningReceiveTasks.TryAdd(receive, receive);

            var receiver = receive.ContinueWith((t, state) =>
            {
                var receiveTasks = (ConcurrentDictionary<Task, Task>) state;
                Task toBeRemoved;
                receiveTasks.TryRemove(t, out toBeRemoved);
            }, runningReceiveTasks, TaskContinuationOptions.ExecuteSynchronously);
            
            return true;
        }

        async Task Receiver(string tableName, CancellationToken cancellationToken)
        {
            var queue = new TableQueue(host.Name, tableName, host.ConnectionString);
            await queue.Receive(cancellationToken);
            concurrencyLimiter.Release();
        }
    }
}