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
        IEnumerable<IQueue> queueList;
        int maxConcurrency = 0;
        TimeSpan retryLoop;
        Action<Exception> ex;

        public void Init(IEnumerable<IQueue> queueList, int maxConcurrency, TimeSpan retryLoop, Action<Exception> ex)
        {
            this.maxConcurrency = maxConcurrency;
            this.queueList = queueList.TakeWhile(x => !cts.IsCancellationRequested);
            this.retryLoop = retryLoop;
            this.ex = ex;
        }

        public void Start()
        {
            cts = new CancellationTokenSource();
            iterator = Task.Run(Receive, CancellationToken.None);
        }

        public async Task Stop()
        {
            cts.Cancel();

            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
            var finishedTask = await Task.WhenAny(iterator, timeoutTask);

            if (finishedTask == timeoutTask)
                throw new TimeoutException("Processor cancel timeout !");

            cts.Dispose();
        }

        async Task Receive()
        {
            while (!cts.IsCancellationRequested)
            {
                queueList
                    .AsParallel()
                    .WithCancellation(cts.Token)
                    .WithDegreeOfParallelism(maxConcurrency)
                    .ForAll(q => q.Receive(cts.Token).ContinueWith(x => {
                        if (x.IsFaulted)
                            ex(x.Exception);
                    }).GetAwaiter().GetResult());

                await Task.Delay(retryLoop, cts.Token);
            }
        }
    }
}