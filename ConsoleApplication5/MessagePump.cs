namespace QueueProcessor
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Concurrent;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    
    class MessagePump
    {
        ConcurrentDictionary<Task, Task> runningReceiveTasks;
        CancellationToken cancellationToken;
        CancellationTokenSource cancellationTokenSource;
        SemaphoreSlim concurrencyLimiter;
        TableBasedQueue inputQueue;
        Task messagePumpTask;
        Func<MessageContext, Task> onMessage;
        Func<ErrorContext, Task<ErrorHandleResult>> onError;
        SqlConnectionFactory sqlConnectionFactory;
        FailureInfoStorage failureInfoStorage;
        static ILogger Logger = LogManager.GetLogger<MessagePump>();

        public async Task Init(SqlConnectionFactory sqlConnectionFactory, TableBasedQueue inputQueue, Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError)
        {
            this.inputQueue = inputQueue;
            this.onMessage = onMessage;
            this.onError = onError;
            this.sqlConnectionFactory =   sqlConnectionFactory;
            this.failureInfoStorage = new FailureInfoStorage(10000);
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

            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(timeoutDurationInSeconds));
            var allTasks = runningReceiveTasks.Values.Concat(new[]
            {
                messagePumpTask,
                //purgeTask
            });
            var finishedTask = await Task.WhenAny(Task.WhenAll(allTasks), timeoutTask).ConfigureAwait(false);

            if (finishedTask.Equals(timeoutTask))
            {
                Logger.LogError("The message pump failed to stop within the time allowed ({0}s)", timeoutDurationInSeconds);
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
                    Logger.LogDebug("Exception thrown during cancellation", e);
                }
                catch (Exception ex)
                {
                    Logger.LogError("Sql Message pump failed", ex);
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
                Logger.LogWarning("Sql receive operation failed.", e);
            }
            catch (Exception ex)
            {
                Logger.LogWarning("Sql receive operation failed.", ex);
            }
            finally
            {
                concurrencyLimiter.Release();
            }
        }

        async Task ReceiveMessage(CancellationTokenSource receiveCancellationTokenSource)
        {
            Message message = null;
            try
            {
                using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted, Timeout = TimeSpan.FromSeconds(30) }, TransactionScopeAsyncFlowOption.Enabled))
                using (var connection = await sqlConnectionFactory.OpenNewConnection().ConfigureAwait(false))
                {
                    message = await inputQueue.TryReceive(connection, null, receiveCancellationTokenSource).ConfigureAwait(false);

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

                    failureInfoStorage.ClearFailureInfoForMessage(message.MessageId.ToString());
                }

            }
            catch (Exception exception)
            {
                if (message == null)
                {
                    throw;
                }

                failureInfoStorage.RecordFailureInfoForMessage(message.MessageId.ToString(), exception);
            }
        }

        protected async Task<ErrorHandleResult> HandleError(Exception exception, Message message, TransactionScope transportTransaction, int processingAttempts)
        {
            try
            {
                var errorContext = new ErrorContext(exception, message, transportTransaction, processingAttempts);

                return await onError(errorContext).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
        //        criticalError.Raise($"Failed to execute reverability actions for message `{message.TransportId}`", ex);

                return ErrorHandleResult.RetryRequired;
            }
        }

        async Task<bool> TryProcess(Message message, TransactionScope transportTransaction)
        {
            FailureInfoStorage.ProcessingFailureInfo failure;
            if (failureInfoStorage.TryGetFailureInfoForMessage(message.MessageId.ToString(), out failure))
            {
                var errorHandlingResult = await HandleError(failure.Exception, message, transportTransaction, failure.NumberOfProcessingAttempts).ConfigureAwait(false);

                if (errorHandlingResult == ErrorHandleResult.Handled)
                {
                    return true;
                }
            }

            try
            {
                var messageProcessed = await TryProcessingMessage(message, transportTransaction).ConfigureAwait(false);
                return messageProcessed;
            }
            catch (Exception exception)
            {
                failureInfoStorage.RecordFailureInfoForMessage(message.MessageId.ToString(), exception);
                return false;
            }
        }

        async Task<bool> TryProcessingMessage(Message message, TransactionScope transportTransaction)
        {
            using (var pushCancellationTokenSource = new CancellationTokenSource())
            {
                var messageContext = new MessageContext(message, transportTransaction, pushCancellationTokenSource);
                await onMessage(messageContext).ConfigureAwait(false);

                // Cancellation is requested when message processing is aborted.
                // We return the opposite value:
                //  - true when message processing completed successfully,
                //  - false when message processing was aborted.
                return !pushCancellationTokenSource.Token.IsCancellationRequested;
            }
        }                  

        async Task<int> Peek(TimeSpan delay, CancellationToken cancellationToken)
        {
            var messageCount = 0;

            try
            {
                using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted }, TransactionScopeAsyncFlowOption.Enabled))
                using (var connection = await sqlConnectionFactory.OpenNewConnection().ConfigureAwait(false))
                {
                    messageCount = await inputQueue.TryPeek(connection, cancellationToken).ConfigureAwait(false);


                    if (messageCount == 0)
                    {
                        Logger.LogDebug($"Input queue empty. Next peek operation will be delayed for {delay}.");
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
                Logger.LogDebug("Exception thrown during cancellation", e);
            }
            catch (Exception ex)
            {
                Logger.LogWarning("Sql peek operation failed", ex);
            }
            return messageCount;
        }       
    }
}
