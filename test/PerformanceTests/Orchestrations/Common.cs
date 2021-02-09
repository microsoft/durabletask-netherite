// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

    static class Common
    {
        public static async Task ParallelForEachAsync<T>(this IEnumerable<T> items, int maxConcurrency, bool useThreadpool, Func<T, Task> action)
        {
            List<Task> tasks;
            if (items is ICollection<T> itemCollection)
            {
                tasks = new List<Task>(itemCollection.Count);
            }
            else
            {
                tasks = new List<Task>();
            }

            using var semaphore = new SemaphoreSlim(maxConcurrency);
            foreach (T item in items)
            {
                tasks.Add(InvokeThrottledAction(item, action, semaphore, useThreadpool));
            }

            await Task.WhenAll(tasks);
        }

        static async Task InvokeThrottledAction<T>(T item, Func<T, Task> action, SemaphoreSlim semaphore, bool useThreadPool)
        {
            await semaphore.WaitAsync();
            try
            {
                if (useThreadPool)
                {
                    await Task.Run(() => action(item));
                }
                else
                {
                    await action(item);
                }
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
}