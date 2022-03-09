// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.RegularExpressions;
    using System.Threading;
  

    /// <summary>
    /// Functionality for estimating memory size.
    /// </summary>
    class TrackedThreads
    {
        readonly static ConcurrentDictionary<int, Thread> threads = new ConcurrentDictionary<int, Thread>();

        public static Thread MakeTrackedThread(Action action, string name)
        {
            Thread thread = null;        
            thread = new Thread(ThreadStart) { Name = name };         
            void ThreadStart()
            {
                threads.TryAdd(thread.ManagedThreadId, thread);
                try
                {
                    action();
                }
                finally
                {
                    threads.TryRemove(thread.ManagedThreadId, out _);
                }
            }
            return thread;
        }

        public static int NumberThreads => threads.Count;

        public static string GetThreadNames()
        {
            return string.Join(",", threads
                .Values
                .GroupBy((thread) => thread.Name)
                .OrderByDescending(group => group.Count())
                .Select(group => $"{group.Key}(x{group.Count()})"));
        }
    }
}
