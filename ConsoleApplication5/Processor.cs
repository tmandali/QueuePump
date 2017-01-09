using System;
using System.Collections.Generic;
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

        Func<IEnumerable<IQueue>> queueList;

        public void Init(Func<IEnumerable<IQueue>> queueList, int maxConcurrency, TimeSpan retryLoop, Action<Exception> ex)
        {
            this.maxConcurrency = maxConcurrency;
            //this.queueList = queueList.TakeWhile(x => !cts.IsCancellationRequested);
            this.retryLoop = retryLoop;
            this.ex = ex;
            this.queueList = queueList;
        }

        public void Start()
        {
            cts = new CancellationTokenSource();
            iterator = Task.Run(Receive, CancellationToken.None);
            System.Diagnostics.Trace.TraceInformation("Processor started ...");
        }

        public async Task Stop()
        {
            cts.Cancel();

            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
            var finishedTask = await Task.WhenAny(iterator, timeoutTask);

            if (finishedTask == timeoutTask)
                throw new TimeoutException("Processor cancel timeout !");

            cts.Dispose();
            System.Diagnostics.Trace.TraceInformation("Processor stoped ...");
        }

        async Task Receive()
        {
            while (!cts.IsCancellationRequested)
            {
                queueList()
                    .AsParallel()
                    .WithCancellation(cts.Token)
                    .WithDegreeOfParallelism(maxConcurrency)
                    .ForAll(q => q.Receive(cts.Token).ContinueWith(x => {
                        if (x.IsFaulted)
                            ex(x.Exception);
                    }).GetAwaiter().GetResult());

                System.Diagnostics.Trace.TraceInformation($"Wait time {retryLoop}");
                await Task.Delay(retryLoop, cts.Token);
            }
        }
    }
}