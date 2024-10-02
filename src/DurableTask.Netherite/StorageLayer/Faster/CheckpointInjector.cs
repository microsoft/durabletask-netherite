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

        internal bool CheckpointDue(LogAccessor<FasterKV.Key, FasterKV.Value> log, out StoreWorker.CheckpointTrigger trigger, out long? compactUntil, FasterTraceHelper traceHelper)
        {
            if (this.handler != null)
            {
                try
                {
                    traceHelper.FasterProgress("CheckpointInjector: running handler");

                    (trigger, compactUntil) = this.handler(log);
                    this.handler = null; // do not run the same handler again

                    traceHelper.FasterProgress($"CheckpointInjector: trigger={trigger} compactUntil={compactUntil}");

                    if (trigger == StoreWorker.CheckpointTrigger.None)
                    {
                        this.SequenceComplete(log, traceHelper);
                    }

                    return (trigger != StoreWorker.CheckpointTrigger.None);
                }
                catch (Exception e)
                {
                    traceHelper.FasterProgress($"CheckpointInjector: handler faulted: {e}");

                    if (this.continuation.TrySetException(e))
                    {
                        traceHelper.FasterProgress("CheckpointInjector: handler continuation released with exception");
                    }
                    else
                    {
                        traceHelper.FasterProgress("CheckpointInjector: handler continuation already progressed");
                    }
                }
            }

            trigger = StoreWorker.CheckpointTrigger.None;
            compactUntil = null;
            return false;
        }    
                  
        internal void SequenceComplete(LogAccessor<FasterKV.Key, FasterKV.Value> log, FasterTraceHelper traceHelper)
        {
            traceHelper.FasterProgress("CheckpointInjector: sequence complete");

            if (this.continuation.TrySetResult(log))
            {
                traceHelper.FasterProgress("CheckpointInjector: handler continuation released");
            }
            else
            {
                traceHelper.FasterProgress("CheckpointInjector: handler continuation already progressed");
            }
        }

        internal void CompactionComplete(IPartitionErrorHandler errorHandler, FasterTraceHelper traceHelper)
        {
            if (this.InjectFaultAfterCompaction)
            {
                errorHandler.HandleError("CheckpointInjector", "inject failure after compaction", null, true, false);
                this.InjectFaultAfterCompaction = false; // do not do this again unless requested again

                if (this.continuation.TrySetResult(null))
                {
                    traceHelper.FasterProgress("CheckpointInjector: handler continuation released");
                }
                else
                {
                    traceHelper.FasterProgress("CheckpointInjector: handler continuation already progressed");
                }
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
