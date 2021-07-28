// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Simple implementation of a concurrent work item queue that consumes round-robin from two producers.
    /// </summary>
    /// <typeparam name="T">The type of elements in the queue.</typeparam>
    class DoubleWorkItemQueue<T> : IDisposable
    {
        readonly ConcurrentQueue<T> workLocal = new ConcurrentQueue<T>();
        readonly ConcurrentQueue<T> workRemote = new ConcurrentQueue<T>();

        readonly SemaphoreSlim count = new SemaphoreSlim(0);

        bool tryLocalFirst = true;

        public int LocalLoad => this.workLocal.Count;

        public void AddLocal(T element)
        {
            this.workLocal.Enqueue(element);
            this.count.Release();
        }

        public void AddRemote(T element)
        {
            this.workRemote.Enqueue(element);
            this.count.Release();
        }

        public void Dispose()
        {
            this.count.Dispose();
        }

        public async ValueTask<T> GetNext(TimeSpan timeout, CancellationToken cancellationToken)
        {
            try
            {
                T result = default;

                bool success = await this.count.WaitAsync((int)timeout.TotalMilliseconds, cancellationToken);

                if (success)
                {
                    if (this.tryLocalFirst)
                    {
                        success = this.workLocal.TryDequeue(out result);
                        if (!success)
                        {
                            success = this.workRemote.TryDequeue(out result);
                        }
                    }
                    else
                    {
                        success = this.workRemote.TryDequeue(out result);
                        if (!success)
                        {
                            success = this.workLocal.TryDequeue(out result);
                        }
                    }

                    // toggle
                    this.tryLocalFirst = !this.tryLocalFirst;

                    // we should always succeed here; but just for the unlikely case that we don't 
                    // (e.g. if concurrent queue implementation is not linearizable),
                    // put the count back up by one if we didn't actually get an element
                    if (!success)
                    {
                        this.count.Release();
                    }
                }

                return result;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return default(T);
            }
        }
    }
}
