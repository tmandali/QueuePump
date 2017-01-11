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
        Action<Processor> iteration;

        //Func<IEnumerable<IQueue>> queueList;

        public void Init(int maxConcurrency, TimeSpan retryLoop, Action<Exception> ex)
        {
            this.maxConcurrency = maxConcurrency;
            this.retryLoop = retryLoop;
            this.ex = ex;
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

            cts.Dispose();
            Trace.TraceInformation("Processor stoped ...");
        }

        async Task Receive()
        {
            while (!cts.IsCancellationRequested)
            {
                GetQueueList()
                    .TakeWhile(x => !cts.IsCancellationRequested)
                    .AsParallel()
                    .WithCancellation(cts.Token)
                    .WithDegreeOfParallelism(maxConcurrency)
                    .ForAll(q => q.Receive(cts.Token).ContinueWith(x => {
                        if (x.IsFaulted)
                            ex(x.Exception);
                    }).GetAwaiter().GetResult());

                Trace.TraceInformation($"Wait time {retryLoop}");
                await Task.Delay(retryLoop, cts.Token);
            }
        }

        public IEnumerable<IQueue> GetQueueList()
        {
            for (int i = 0; i < ConfigurationManager.ConnectionStrings.Count; i++)
            {
                var hostName = ConfigurationManager.ConnectionStrings[i].Name;
                if (hostName.Split('.')[0] != "QueueHost")
                    continue;

                Trace.TraceInformation($"Lissen to {hostName}");

                using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings[i].ConnectionString))
                {
                    connection.Open();                    
                    var command = new SqlCommand("SELECT [Table], [ConnectionString] FROM [Queue]", connection);
                    var dataReader = command.ExecuteReader();
                    while (dataReader.Read())
                    {
                        var tableName = dataReader.GetFieldValue<string>(0);
                        var connectionString = dataReader.GetFieldValue<string>(1);
                        yield return new TableQueue(tableName, connectionString);
                    }
                }
            }
        }
    }
}