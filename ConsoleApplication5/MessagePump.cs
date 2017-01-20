using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace QueueProcessor
{
    public class MessagePump
    {
        ConcurrentDictionary<Task, Task> runningReceiveTasks;
        CancellationToken cancellationToken;
        CancellationTokenSource cancellationTokenSource;
        SemaphoreSlim concurrencyLimiter;
        string inputQueue;
        Task messagePumpTask;
        Func<MessageContext, Task> onMessage;
        SqlConnectionFactory connectionFactory;

        public async Task Init(Func<MessageContext, Task> onMessage, string InputQueue, string connection)
        {
            this.inputQueue = InputQueue;
            this.onMessage = onMessage;
            this.connectionFactory = SqlConnectionFactory.Default(connection);
        }

        public void Start(int? maxConcurrency = null)
        {
            var concurrency = maxConcurrency ?? Environment.ProcessorCount;

            runningReceiveTasks = new ConcurrentDictionary<Task, Task>();
            concurrencyLimiter = new SemaphoreSlim(concurrency);
            cancellationTokenSource = new CancellationTokenSource();
            cancellationToken = cancellationTokenSource.Token;

            messagePumpTask = Task.Run(ProcessMessages, CancellationToken.None);
        }

        public async Task Stop()
        {
            const int timeoutDurationInSeconds = 30;
            cancellationTokenSource.Cancel();

            // ReSharper disable once MethodSupportsCancellation
            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(timeoutDurationInSeconds));
            var allTasks = runningReceiveTasks.Values.Concat(new[]
            {
                messagePumpTask,
                //purgeTask
            });
            var finishedTask = await Task.WhenAny(Task.WhenAll(allTasks), timeoutTask).ConfigureAwait(false);

            if (finishedTask.Equals(timeoutTask))
            {
                //Logger.ErrorFormat("The message pump failed to stop within the time allowed ({0}s)", timeoutDurationInSeconds);
            }

            concurrencyLimiter.Dispose();
            cancellationTokenSource.Dispose();

            runningReceiveTasks.Clear();
        }

        async Task ProcessMessages()
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await InnerProcessMessages().ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // For graceful shutdown purposes
                }
                catch (SqlException e) when (cancellationToken.IsCancellationRequested)
                {
                    
                }
                catch (Exception ex)
                {

                }
            }
        }

        async Task InnerProcessMessages()
        {
            while (!cancellationTokenSource.IsCancellationRequested)
            {
                var messageCount = await Peek(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);

                if (messageCount == 0)
                {
                    continue;
                }

                if (cancellationTokenSource.IsCancellationRequested)
                {
                    return;
                }

                // We cannot dispose this token source because of potential race conditions of concurrent receives
                var loopCancellationTokenSource = new CancellationTokenSource();

                for (var i = 0; i < messageCount; i++)
                {
                    if (loopCancellationTokenSource.Token.IsCancellationRequested)
                    {
                        break;
                    }

                    await concurrencyLimiter.WaitAsync(cancellationToken).ConfigureAwait(false);

                    var receiveTask = InnerReceive(loopCancellationTokenSource);
                    runningReceiveTasks.TryAdd(receiveTask, receiveTask);

                    var running = receiveTask.ContinueWith((t, state) =>
                    {
                        var receiveTasks = (ConcurrentDictionary<Task, Task>)state;
                        Task toBeRemoved;
                        receiveTasks.TryRemove(t, out toBeRemoved);
                    }, runningReceiveTasks, TaskContinuationOptions.ExecuteSynchronously);                    
                }
            }
        }

        async Task InnerReceive(CancellationTokenSource loopCancellationTokenSource)
        {
            try
            {
                // We need to force the method to continue asynchronously because SqlConnection
                // in combination with TransactionScope will apply connection pooling and enlistment synchronous in ctor.
                await Task.Yield();

                await ReceiveMessage(loopCancellationTokenSource).ConfigureAwait(false);

            }
            catch (SqlException e) when (e.Number == 1205)
            {
                //Receive has been victim of a lock resolution
            }
            catch (Exception ex)
            {

            }
            finally
            {
                concurrencyLimiter.Release();
            }
        }

        async Task ReceiveMessage(CancellationTokenSource receiveCancellationTokenSource)
        {
            ExpandoObject message = null;
            try
            {
                using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, TimeSpan.FromSeconds(1), TransactionScopeAsyncFlowOption.Enabled))
                using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
                {
                    message = await TryReceive(connection, null, receiveCancellationTokenSource).ConfigureAwait(false);

                    if (message == null)
                    {
                        // The message was received but is not fit for processing (e.g. was DLQd).
                        // In such a case we still need to commit the transport tx to remove message
                        // from the queue table.
                        scope.Complete();
                        return;
                    }

                    connection.Close();

                    if (!await TryProcess(message, scope).ConfigureAwait(false))
                    {
                        return;
                    }

                    scope.Complete();
                }

            }
            catch (Exception exception)
            {
                if (message == null)
                {
                    throw;
                }
            }
        }

        async Task<bool> TryProcess(ExpandoObject message, TransactionScope transportTransaction)
        {
            using (var pushCancellationTokenSource = new CancellationTokenSource())
            {
                var messageContext = new MessageContext(message, transportTransaction);

                await onMessage(messageContext).ConfigureAwait(false);

                // Cancellation is requested when message processing is aborted.
                // We return the opposite value:
                //  - true when message processing completed successfully,
                //  - false when message processing was aborted.
                return !pushCancellationTokenSource.Token.IsCancellationRequested;
            }
        }

        async Task<ExpandoObject> TryReceive(SqlConnection connection, SqlTransaction transaction, CancellationTokenSource receiveCancellationTokenSource)
        {
            var commandText = $"select top 1 * from {inputQueue}"; 

            using (var command = new SqlCommand(commandText, connection, transaction))
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

            var properyBag = (ICollection<KeyValuePair<string, object>>) result;
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

        async Task<int> Peek(TimeSpan delay, CancellationToken cancellationToken)
        {
            var messageCount = 0;

            try
            {
                using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted }, TransactionScopeAsyncFlowOption.Enabled))
                using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
                {
                    messageCount = await TryPeek(connection, cancellationToken).ConfigureAwait(false);


                    if (messageCount == 0)
                    {
                    //    Logger.Debug($"Input queue empty. Next peek operation will be delayed for {settings.Delay}.");

                        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    }

                    scope.Complete();
                }
            }
            catch (OperationCanceledException)
            {
                //Graceful shutdown
            }
            catch (SqlException e) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
            }

            return messageCount;
        }

        async Task<int> TryPeek(SqlConnection connection, CancellationToken token, int timeoutInSeconds = 30)
        {
            var commandText = $"select count(*) from {inputQueue}";//Format(Sql.PeekText, schemaName, tableName);

            using (var command = new SqlCommand(commandText, connection)
            {
                CommandTimeout = timeoutInSeconds
            })
            {
                var numberOfMessages = (int)await command.ExecuteScalarAsync(token).ConfigureAwait(false);

                return numberOfMessages;
            }
        }
    }
}
