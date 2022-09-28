// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using FASTER.core;
    using Microsoft.Azure.Storage;

    /// <summary>
    /// Injects checkpoint and compaction decisions into the store worker. 
    /// The intention is to override the usual heuristics, so the test can control when checkpoints and compaction happen.
    /// </summary>
    public class CheckpointInjector
    {
        readonly TestHooks testHooks;

        internal delegate (StoreWorker.CheckpointTrigger reason, long? compactUntil) CheckpointDueAsync(LogAccessor<FasterKV.Key, FasterKV.Value> log);

        CheckpointDueAsync handler;
        TaskCompletionSource<LogAccessor<FasterKV.Key, FasterKV.Value>> continuation;

        public bool InjectFaultAfterCompaction { get; private set; }

        public CheckpointInjector(TestHooks testHooks)
        {
            this.testHooks = testHooks;
        }

        internal bool CheckpointDue(LogAccessor<FasterKV.Key, FasterKV.Value> log, out StoreWorker.CheckpointTrigger trigger, out long? compactUntil)
        {
            if (this.handler != null)
            {
                try
                {
                    (trigger, compactUntil) = this.handler(log);
                    this.handler = null;

                    if (trigger == StoreWorker.CheckpointTrigger.None)
                    {
                        this.SequenceComplete(log);
                    }
                } 
                catch(Exception e)
                {
                    this.continuation.SetException(e);
                    this.continuation = null;
                    throw;
                }
            }
            else
            {
                trigger = StoreWorker.CheckpointTrigger.None;
                compactUntil = null;
            }

            return (trigger != StoreWorker.CheckpointTrigger.None);
        }

        internal void SequenceComplete(LogAccessor<FasterKV.Key, FasterKV.Value> log)
        {
            this.continuation?.SetResult(log);
            this.continuation = null;
        }

        internal void CompactionComplete(IPartitionErrorHandler errorHandler)
        {
            if (this.InjectFaultAfterCompaction)
            {
                errorHandler.HandleError("CheckpointInjector", "inject failure after compaction", null, true, false);
                this.InjectFaultAfterCompaction = false;
                this.continuation?.SetResult(null);
                this.continuation = null;
            }
        }

        internal Task<LogAccessor<FasterKV.Key, FasterKV.Value>> InjectAsync(CheckpointDueAsync handler, bool injectFailureAfterCompaction = false)
        {
            this.continuation = new TaskCompletionSource<LogAccessor<FasterKV.Key, FasterKV.Value>>(TaskCreationOptions.RunContinuationsAsynchronously);
            this.handler = handler;
            this.InjectFaultAfterCompaction = injectFailureAfterCompaction;
            return this.continuation.Task;
        }
    }
}
