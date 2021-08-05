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

        const int REMOTE_BUFFER_BOUND = 100;

        readonly SemaphoreSlim availableForDequeue = new SemaphoreSlim(0);
        readonly SemaphoreSlim availableForRemoteEnqueue = new SemaphoreSlim(REMOTE_BUFFER_BOUND);

        bool tryLocalFirst = true;

        public int LocalLoad => this.workLocal.Count;

        public void AddLocal(T element)
        {
            this.workLocal.Enqueue(element);
            this.availableForDequeue.Release();
        }

        public async ValueTask AddRemoteAsync(T element)
        {
            await this.availableForRemoteEnqueue.WaitAsync();
            this.workRemote.Enqueue(element);
            this.availableForDequeue.Release();
        }

        public void Dispose()
        {
            this.availableForDequeue.Dispose();
            this.availableForRemoteEnqueue.Dispose();
        }

        public async ValueTask<T> GetNext(TimeSpan timeout, CancellationToken cancellationToken)
        {
            try
            {
                T result = default;

                bool success = await this.availableForDequeue.WaitAsync((int)timeout.TotalMilliseconds, cancellationToken);

                if (success)
                {
                    if (this.tryLocalFirst)
                    {
                        success = this.workLocal.TryDequeue(out result);
                        if (!success)
                        {
                            success = this.workRemote.TryDequeue(out result);
                            if (success)
                            {
                                this.availableForRemoteEnqueue.Release();
                            }
                        }
                    }
                    else
                    {
                        success = this.workRemote.TryDequeue(out result);
                        if (success)
                        {
                            this.availableForRemoteEnqueue.Release();
                        }
                        else
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
                        this.availableForDequeue.Release();
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
