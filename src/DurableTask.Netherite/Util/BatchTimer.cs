// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    class BatchTimer<T>
    {
        readonly CancellationToken cancellationToken;
        readonly Action<List<T>> handler;
        readonly SortedList<(DateTime due, int id), T> schedule;
        readonly SemaphoreSlim notify;
        readonly Action<string> tracer;
        readonly object thisLock; //TODO replace this class with a lock-free implementation
        string name;

        volatile int sequenceNumber;

        public BatchTimer(CancellationToken token, Action<List<T>> handler, Action<string> tracer = null)
        {
            this.cancellationToken = token;
            this.handler = handler;
            this.tracer = tracer;
            this.schedule = new SortedList<(DateTime due, int id), T>();
            this.notify = new SemaphoreSlim(0, int.MaxValue);
            this.thisLock = new object();

            token.Register(() => this.notify.Release());
        }

        public void Start(string name)
        {
            var thread = new Thread(this.ExpirationCheckLoop);
            thread.Name = name;
            this.name = name;
            thread.Start();
        }

        public int GetFreshId()
        {
            lock (this.thisLock)
            {
                return this.sequenceNumber++;
            }
        }

        public void Schedule(DateTime due, T what, int? id = null)
        {
            lock (this.thisLock)
            {
                var key = (due, id ?? this.sequenceNumber++);

                this.schedule.Add(key, what);

                this.tracer?.Invoke($"{this.name} scheduled ({key.Item1:o},{key.Item2})");

                // notify the expiration check loop
                if (key == this.schedule.First().Key)
                {
                    this.notify.Release();
                }
            }
        }

        public bool TryCancel((DateTime due, int id) key)
        {
            lock (this.thisLock)
            {
                if (this.schedule.Remove(key))
                {
                    this.tracer?.Invoke($"{this.name} canceled ({key.due:o},{key.id})");
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        void ExpirationCheckLoop()
        {
            List<T> batch = new List<T>();
            (DateTime due, int id) firstInBatch = default;
            (DateTime due, int id) nextAfterBatch = default;

            while (!this.cancellationToken.IsCancellationRequested)
            {
                // wait for the next expiration time or cleanup, but cut the wait short if notified
                if (this.RequiresDelay(out var delay, out var due))
                {
                    var startWait = DateTime.UtcNow;
                    this.notify.Wait(delay); // blocks thread until delay is over, or until notified                 
                    this.tracer?.Invoke($"{this.name} is awakening at {(DateTime.UtcNow - due).TotalSeconds}s");
                }

                lock (this.thisLock)
                {
                    var next = this.schedule.FirstOrDefault();

                    while (this.schedule.Count > 0
                        && next.Key.due <= DateTime.UtcNow
                        && !this.cancellationToken.IsCancellationRequested)
                    {
                        this.schedule.RemoveAt(0);
                        batch.Add(next.Value);

                        if (batch.Count == 1)
                        {
                            firstInBatch = next.Key;
                        }

                        next = this.schedule.FirstOrDefault();
                    }

                    nextAfterBatch = next.Key;
                }

                if (batch.Count > 0)
                {
                    this.tracer?.Invoke($"starting {this.name} batch size={batch.Count} first=({firstInBatch.due:o},{firstInBatch.id}) next=({nextAfterBatch.due:o},{nextAfterBatch.id})");

                    // it is expected that the handler catches 
                    // all exceptions, since it has more meaningful ways to report errors
                    this.handler(batch);
                    batch.Clear();

                    this.tracer?.Invoke($"completed {this.name} batch size={batch.Count} first=({firstInBatch.due:o},{firstInBatch.id}) next=({nextAfterBatch.due:o},{nextAfterBatch.id})");
                }
            }
        }

        bool RequiresDelay(out TimeSpan delay, out DateTime due)
        {
            lock (this.thisLock)
            {
                if (this.schedule.Count == 0)
                {
                    delay = TimeSpan.FromMilliseconds(-1); // represents infinite delay
                    due = DateTime.MaxValue;
                    return true;
                }

                var next = this.schedule.First();
                var now = DateTime.UtcNow;

                if (next.Key.due > now)
                {
                    due = next.Key.due;
                    delay = due - now;
                    return true;
                }
                else
                {
                    due = now;
                    delay = default;
                    return false;
                }
            }
        }
    }
}
