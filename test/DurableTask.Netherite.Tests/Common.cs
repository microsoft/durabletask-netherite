// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    static class Common
    {
        public static async Task<List<S>> ParallelForEachAsync<T,S>(this IEnumerable<T> items, int maxConcurrency, bool useThreadpool, Func<T, Task<S>> action)
        {
            List<Task<S>> tasks;
            if (items is ICollection<T> itemCollection)
            {
                tasks = new List<Task<S>>(itemCollection.Count);
            }
            else
            {
                tasks = new List<Task<S>>();
            }

            using var semaphore = new SemaphoreSlim(maxConcurrency);
            foreach (T item in items)
            {
                tasks.Add(InvokeThrottledAction(item, action, semaphore, useThreadpool));
            }

            await Task.WhenAll(tasks);
            return tasks.Select(t => t.Result).ToList();
        }

        static async Task<S> InvokeThrottledAction<T,S>(T item, Func<T, Task<S>> action, SemaphoreSlim semaphore, bool useThreadPool)
        {
            await semaphore.WaitAsync();
            try
            {
                if (useThreadPool)
                {
                    return await Task.Run(() => action(item));
                }
                else
                {
                    return await action(item);
                }
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
}