// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// General pattern for an asynchronous worker that performs a work task, when notified,
    /// to service queued work on the thread pool. Each work cycle handles ALL the queued work. 
    /// If new work arrives during a work cycle, another cycle is scheduled. 
    /// The worker never executes more than one instance of the work cycle at a time, 
    /// and consumes no thread or task resources when idle.
    /// </summary>
    abstract class BatchWorker<T>
    {
        readonly Stopwatch stopwatch;
        protected readonly CancellationToken cancellationToken;

        volatile int state;
        const int IDLE = 0;
        const int RUNNING = 1;
        const int SUSPENDED = 2;
        const int SHUTTINGDOWN = 3;
        readonly ConcurrentQueue<object> work = new ConcurrentQueue<object>();

        const int MAXBATCHSIZE = 10000;
        readonly object dummyEntry = new object();

        /// <summary>
        /// Constructor including a cancellation token.
        /// </summary>
        public BatchWorker(string name, bool startSuspended, CancellationToken cancellationToken)
        {
            this.cancellationToken = cancellationToken;
            this.state = startSuspended ? SUSPENDED : IDLE;
            this.stopwatch = new Stopwatch();
        }

        /// <summary>Implement this member in derived classes to process a batch</summary>
        protected abstract Task Process(IList<T> batch);

        public void Submit(T entry)
        {
            this.work.Enqueue(entry);
            this.NotifyInternal();
        }

        public void SubmitBatch(IList<T> entries)
        {
            foreach (var e in entries)
            {
                this.work.Enqueue(e);
            }
            this.NotifyInternal();
        }

        public void Notify()
        {
            this.work.Enqueue(this.dummyEntry);
            this.NotifyInternal();
        }

        protected void Requeue(IList<T> entries)
        {
            this.requeued = entries;
        }

        public virtual Task WaitForCompletionAsync()
        {
            var tcs = new TaskCompletionSource<bool>();
            this.work.Enqueue(tcs);
            this.Notify();
            return tcs.Task;
        }

        readonly List<T> batch = new List<T>();
        readonly List<TaskCompletionSource<bool>> waiters = new List<TaskCompletionSource<bool>>();
        IList<T> requeued = null;

        int? GetNextBatch()
        {
            bool runAgain = false;

            this.batch.Clear();
            this.waiters.Clear();

            if (this.requeued != null)
            {
                runAgain = true;
                this.batch.AddRange(this.requeued);
                this.requeued = null;
            }

            while (this.batch.Count < MAXBATCHSIZE)
            {
                if (!this.work.TryDequeue(out object entry))
                {
                    break;
                }
                else if (entry is T t)
                {
                    runAgain = true;
                    this.batch.Add(t);
                }
                else if (entry == this.dummyEntry)
                {
                    runAgain = true;
                    continue;
                }
                else if (entry is SemaphoreSlim credits)
                {
                    runAgain = true;
                    credits.Release();
                }
                else
                {
                    runAgain = true;
                    this.waiters.Add((TaskCompletionSource<bool>)entry);
                }
            }

            return runAgain ? this.batch.Count : (int?)null;
        }

        async Task Work()
        {
            EventTraceContext.Clear();
            int? previousBatch = null;

            while(!this.cancellationToken.IsCancellationRequested)
            {
                int? nextBatch = this.GetNextBatch();

                if (previousBatch.HasValue)
                {
                    this.WorkLoopCompleted(previousBatch.Value, this.stopwatch.Elapsed.TotalMilliseconds, nextBatch);
                }

                if (!nextBatch.HasValue)
                {
                    // no work found. Announce that we are planning to shut down.
                    // Using an interlocked exchange to enforce store-load fence.
                    int read = Interlocked.Exchange(ref this.state, SHUTTINGDOWN);

                    Debug.Assert(read == RUNNING); // because that's what its set to before Work is called

                    // but recheck so we don't miss work that was just added
                    nextBatch = this.GetNextBatch();

                    if (!nextBatch.HasValue)
                    {
                        // still no work. Try to transition to idle but revert if state has been changed.
                        read = Interlocked.CompareExchange(ref this.state, IDLE, SHUTTINGDOWN);

                        if (read == SHUTTINGDOWN)
                        {
                            // shut down is complete
                            return;
                        }
                        else
                        {
                            // shutdown was reverted by Notify()
                            Debug.Assert(read == RUNNING);
                        }
                    }
                    else
                    {
                        // we found more work. So we do not shutdown but go back to running.
                        this.state = RUNNING;
                    }
                }

                this.stopwatch.Restart();

                try
                {
                    // recheck for cancellation right before doing work
                    this.cancellationToken.ThrowIfCancellationRequested();

                    // do the work, calling the virtual method
                    await this.Process(this.batch).ConfigureAwait(false);

                    // notify anyone who is waiting for completion
                    foreach (var w in this.waiters)
                    {
                        w.TrySetResult(true);
                    }
                }
                catch (Exception e)
                {
                    foreach (var w in this.waiters)
                    {
                        w.TrySetException(e);
                    }
                }

                this.stopwatch.Stop();

                previousBatch = this.batch.Count;
            }
        }

        /// <summary>
        /// Can be overridden by subclasses to trace progress of the batch work loop
        /// </summary>
        /// <param name="batchSize">The size of the batch that was processed</param>
        /// <param name="elapsedMilliseconds">The time in milliseconds it took to process</param>
        /// <param name="nextBatch">If there is more work, the current queue size of the next batch, or null otherwise</param>
        protected virtual void WorkLoopCompleted(int batchSize, double elapsedMilliseconds, int? nextBatch)
        {
        }

        public void Resume()
        {
            this.work.Enqueue(this.dummyEntry);
            this.NotifyInternal(true);
        }

        void NotifyInternal(bool resume = false)
        {
            while (true)
            {
                int currentState = this.state;
                if (currentState == RUNNING)
                {
                    return; // worker is already running
                }
                else if (currentState == SUSPENDED && !resume)
                {
                    return; // we do not want to start processing yet
                }
                else 
                {
                    int read = Interlocked.CompareExchange(ref this.state, RUNNING, currentState);
                    if (read == currentState)
                    {
                        if (read != SHUTTINGDOWN)
                        {
                            Task.Run(this.Work);
                        }
                        break;
                    }
                    else
                    {
                        continue;
                    }
                }
            }
        }
    }
}