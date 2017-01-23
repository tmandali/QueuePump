namespace QueueProcessor
{
    using System;
    using System.Collections.Concurrent;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    
    public class MessagePump
    {
        ConcurrentDictionary<Task, Task> runningReceiveTasks;
        CancellationToken cancellationToken;
        CancellationTokenSource cancellationTokenSource;
        SemaphoreSlim concurrencyLimiter;
        TableBaseQueue inputQueue;
        Task messagePumpTask;
        Func<MessageContext, Task> onMessage;
        Func<ErrorContext, Task<ErrorHandleResult>> onError;
        Func<Task> onComplete;
        SqlConnectionFactory connectionFactory;
        FailureInfoStorage failureInfoStorage;

        public async Task Init(Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError, Func<Task> onComplete, TableBaseQueue inputQueue, string connectionString)
        {
            this.inputQueue = inputQueue;
            this.onMessage = onMessage;
            this.onError = onError;
            this.onComplete = onComplete;
            this.connectionFactory = SqlConnectionFactory.Default(connectionString);
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

                await Complete(cancellationToken).ConfigureAwait(false);
            }
        }

        async Task Complete(CancellationToken cancellationToken)
        {
            try
            {
                await onComplete().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                
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
            Message message = null;
            try
            {
                using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted, Timeout = TimeSpan.FromSeconds(30) }, TransactionScopeAsyncFlowOption.Enabled))
                using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
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
                using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
                {
                    messageCount = await inputQueue.TryPeek(connection, cancellationToken).ConfigureAwait(false);


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
    }
}
