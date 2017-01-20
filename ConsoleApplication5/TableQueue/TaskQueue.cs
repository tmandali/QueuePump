using System;
using System.Threading;
using System.Threading.Tasks;

namespace QueueProcessor
{
    public class TaskQueue
    {
        private SemaphoreSlim semaphore;
        public TaskQueue(int maxCount)
        {
            semaphore = new SemaphoreSlim(maxCount);
        }

        public async Task<T> Enqueue<T>(Func<Task<T>> taskGenerator)
        {
            await semaphore.WaitAsync();
            try
            {
                return await taskGenerator();
            }
            finally
            {
                semaphore.Release();
            }
        }
        public async Task Enqueue(Func<Task> taskGenerator)
        {
            await semaphore.WaitAsync();
            try
            {
                await taskGenerator();
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
}
