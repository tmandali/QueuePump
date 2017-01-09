using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QueueProcessor
{
    public static class ParallelTaskRunnerExtension
    {
        public static void RunTasks(this IEnumerable<Task> tasks, int maxConcurrency, Action<Task> taskComplete = null)
        {
            if (maxConcurrency <= 0) throw new ArgumentException("maxConcurrency must be more than 0.");
            int taskCount = 0;
            int nextIndex = 0;
            var currentTasks = new Task[maxConcurrency];

            foreach (var task in tasks)
            {
                currentTasks[nextIndex] = task;
                taskCount++;
                if (taskCount == maxConcurrency)
                {
                    nextIndex = Task.WaitAny(currentTasks);
                    if (taskComplete != null)
                    {
                        taskComplete(currentTasks[nextIndex]);
                    }
                    currentTasks[nextIndex] = null;
                    taskCount--;
                }
                else
                {
                    nextIndex++;
                }
            }

            while (taskCount > 0)
            {
                currentTasks = currentTasks.Where(t => t != null).ToArray();
                nextIndex = Task.WaitAny(currentTasks);
                if (taskComplete != null)
                {
                    taskComplete(currentTasks[nextIndex]);
                }
                currentTasks[nextIndex] = null;
                taskCount--;
            }
        }

        public static void RunTasks<T>(this IEnumerable<Task<T>> tasks, int maxConcurrency, Action<Task<T>> taskComplete = null)
        {
            if (maxConcurrency <= 0) throw new ArgumentException("maxConcurrency must be more than 0.");
            int taskCount = 0;
            int nextIndex = 0;
            var currentTasks = new Task<T>[maxConcurrency];

            foreach (var task in tasks)
            {
                currentTasks[nextIndex] = task;
                taskCount++;
                if (taskCount == maxConcurrency)
                {
                    nextIndex = Task.WaitAny(currentTasks);
                    if (taskComplete != null)
                    {
                        taskComplete(currentTasks[nextIndex]);
                    }
                    currentTasks[nextIndex] = null;
                    taskCount--;
                }
                else
                {
                    nextIndex++;
                }
            }

            while (taskCount > 0)
            {
                currentTasks = currentTasks.Where(t => t != null).ToArray();
                nextIndex = Task.WaitAny(currentTasks);
                if (taskComplete != null)
                {
                    taskComplete(currentTasks[nextIndex]);
                }
                currentTasks[nextIndex] = null;
                taskCount--;
            }
        }
    }
}
