﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Faster;

    /// <summary>
    /// General pattern for an asynchronous worker that performs a work task, when notified,
    /// to service queued work on the thread pool. Each work cycle handles ALL the queued work. 
    /// If new work arrives during a work cycle, another cycle is scheduled. 
    /// The worker never executes more than one instance of the work cycle at a time, 
    /// and consumes no thread or task resources when idle.
    /// </summary>
    abstract class BatchWorker<T>
    {
        readonly int maxBatchSize;
        readonly Stopwatch stopwatch;
        protected string Name { get; }
        protected readonly CancellationToken cancellationToken;
        readonly IBatchWorkerTraceHelper traceHelper;

        volatile int state;
        const int IDLE = 0;
        const int RUNNING = 1;
        const int SUSPENDED = 2;
        const int SHUTTINGDOWN = 3;
        readonly ConcurrentQueue<object> work = new ConcurrentQueue<object>();

        const int MAXBATCHSIZE = 500;
        readonly object dummyEntry = new object();

        bool processingBatch;
        public TimeSpan? ProcessingBatchSince => this.processingBatch ? this.stopwatch.Elapsed : null;

        volatile TaskCompletionSource<object> shutdownCompletionSource;

        /// <summary>
        /// Constructor including a cancellation token.
        /// </summary>
        public BatchWorker(string name, bool startSuspended, int maxBatchSize, CancellationToken cancellationToken, IBatchWorkerTraceHelper traceHelper)
        {
            this.Name = name;
            this.cancellationToken = cancellationToken;
            this.state = startSuspended ? SUSPENDED : IDLE;
            this.maxBatchSize = maxBatchSize;
            this.stopwatch = new Stopwatch();
            this.traceHelper = traceHelper;
        }

        public bool IsIdle => (this.state == IDLE);

        protected Action<string> Tracer { get; set; } = null;

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
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            this.work.Enqueue(tcs);
            this.Notify();
            return tcs.Task;
        }

        public virtual Task WaitForShutdownAsync()
        {
            if (!this.cancellationToken.IsCancellationRequested)
            {
                throw new InvalidOperationException("must call this only after canceling the token");
            }

            if (this.shutdownCompletionSource == null)
            {
                Interlocked.CompareExchange(ref this.shutdownCompletionSource, new TaskCompletionSource<object>(), null);
                this.NotifyInternal();
            }

            return this.shutdownCompletionSource.Task;
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
            this.Tracer?.Invoke("started");

            EventTraceContext.Clear();
            int? previousBatch = null;

            while(!this.cancellationToken.IsCancellationRequested)
            {
                int? nextBatch = this.GetNextBatch();

                if (previousBatch.HasValue && previousBatch.Value > 0)
                {
                    this.traceHelper?.TraceBatchWorkerProgress(this.Name, previousBatch.Value, this.stopwatch.Elapsed.TotalMilliseconds, nextBatch);
                }

                if (!nextBatch.HasValue)
                {
                    this.Tracer?.Invoke("no work, starting to shut down");

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
                            this.Tracer?.Invoke("shut down");
                            return;
                        }
                        else
                        {
                            // shutdown was reverted by Notify()
                            Debug.Assert(read == RUNNING);
                            this.Tracer?.Invoke("was told to continue");
                            continue;
                        }
                    }
                    else
                    {
                        // we found more work. So we do not shutdown but go back to running.
                        this.Tracer?.Invoke("found more work after all");
                        this.state = RUNNING;
                    }
                }

                this.Tracer?.Invoke("processing batch");
                this.stopwatch.Restart();
                this.processingBatch = true;

                try
                {
                    // recheck for cancellation right before doing work
                    this.cancellationToken.ThrowIfCancellationRequested();

                    // do the work, calling the virtual method
                    await this.Process(this.batch).ConfigureAwait(false);

                    this.Tracer?.Invoke("notifying waiters");

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

                this.Tracer?.Invoke("done processing batch");
                this.stopwatch.Stop();
                this.processingBatch = false;
                previousBatch = this.batch.Count;
            }

            if (this.cancellationToken.IsCancellationRequested)
            {
                this.shutdownCompletionSource?.TrySetResult(null);
            }
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
                    this.Tracer?.Invoke("already running");
                    return; // worker is already running
                }
                else if (currentState == SUSPENDED && !resume)
                {
                    this.Tracer?.Invoke("suspended");
                    return; // we do not want to start processing yet
                }
                else 
                {
                    int read = Interlocked.CompareExchange(ref this.state, RUNNING, currentState);
                    if (read == currentState)
                    {
                        if (read != SHUTTINGDOWN)
                        {
                            this.Tracer?.Invoke("launching");
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

    /// <summary>
    /// interface for supporting the same kind of tracing by all the batch workers
    /// </summary>
    public interface IBatchWorkerTraceHelper
    {
        public void TraceBatchWorkerProgress(string worker, int batchSize, double elapsedMilliseconds, int? nextBatch);
    }
}