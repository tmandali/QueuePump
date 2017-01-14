using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace QueueProcessor
{
    class Processor
    {
        Task iterator;
        CancellationTokenSource cts;
        int maxConcurrency = 0;
        TimeSpan retryLoop;
        Action<Exception> ex;
        SemaphoreSlim concurrencyLimiter;

        public void Init(int maxConcurrency, TimeSpan retryLoop, Action<Exception> ex)
        {
            this.maxConcurrency = maxConcurrency;
            this.retryLoop = retryLoop;
            this.ex = ex;
            this.concurrencyLimiter = new SemaphoreSlim(maxConcurrency);
        }

        public void Start()
        {
            cts = new CancellationTokenSource();
            iterator = Task.Run(Receive, CancellationToken.None);
            Trace.TraceInformation("Processor started ...");
        }

        public async Task Stop()
        {
            cts.Cancel();

            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
            var finishedTask = await Task.WhenAny(iterator, timeoutTask);

            if (finishedTask == timeoutTask)
                throw new TimeoutException("Processor cancel timeout !");

            concurrencyLimiter.Dispose();
            cts.Dispose();
            Trace.TraceInformation("Processor stoped ...");
        }

        static void Ignore(Task task)
        {

        }

        async Task Receive()
        {
            while (!cts.IsCancellationRequested)
            {
                var tasklist = new System.Collections.Concurrent.ConcurrentBag<Task>();

                foreach (ConnectionStringSettings connectionSetting in ConfigurationManager.ConnectionStrings)
                {
                    if (connectionSetting.Name.Split('.')[0] != "QueueHost")
                        continue;

                    //Trace.TraceInformation($"Lissen to {connectionSetting.Name}");

                    using (var connection = await SqlConnectionFactory.Default(connectionSetting.ConnectionString).OpenNewConnection().ConfigureAwait(false))
                    {
                        var command = new SqlCommand("SELECT [Table], [MaxConcurrency] FROM [Queue]", connection);
                        var dataReader = await command.ExecuteReaderAsync().ConfigureAwait(false);
                        while (await dataReader.ReadAsync().ConfigureAwait(false))
                        {
                            var tableName = await dataReader.GetFieldValueAsync<string>(0).ConfigureAwait(false);
                            var maxConcurrency = await dataReader.GetFieldValueAsync<int>(1).ConfigureAwait(false);
                            var instance = new TableQueue(
                                connectionSetting.Name,
                                tableName,
                                connection.ConnectionString);

                            for (int i = 0; i < maxConcurrency; i++)
                            {
                                var task = instance.Receive(cts.Token);
                                tasklist.Add(task);
                            }
                        }
                    }
                }

                Task runner;
                while (tasklist.TryTake(out runner))
                {
                    await concurrencyLimiter.WaitAsync(cts.Token).ConfigureAwait(false);
                    await runner.ContinueWith(c => concurrencyLimiter.Release(), cts.Token).ConfigureAwait(false);
                }

                Trace.TraceInformation($"Wait time {retryLoop}");
                await Task.Delay(retryLoop, cts.Token).ConfigureAwait(false);
            }
        }
    }
}