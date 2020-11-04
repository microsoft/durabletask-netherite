// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;

    /// <summary>
    /// Is used while applying an effect to a partition state, to carry
    /// information about the context, and to enumerate the objects on which the effect
    /// is being processed.
    /// </summary>
    class EffectTracker : List<TrackedObjectKey>
    {
        readonly Func<TrackedObjectKey, EffectTracker, ValueTask> applyToStore;
        readonly Func<(long, long)> getPositions;
        readonly System.Diagnostics.Stopwatch stopWatch;

        public EffectTracker(Partition partition, Func<TrackedObjectKey, EffectTracker, ValueTask> applyToStore, Func<(long, long)> getPositions)
        {
            this.Partition = partition;
            this.applyToStore = applyToStore;
            this.getPositions = getPositions;
            this.stopWatch = new System.Diagnostics.Stopwatch();
            this.stopWatch.Start();
        }

        /// <summary>
        /// The current partition.
        /// </summary>
        public Partition Partition { get; }

        /// <summary>
        /// The effect currently being applied.
        /// </summary>
        public dynamic Effect { get; set; }

        /// <summary>
        /// True if we are replaying this effect during recovery.
        /// Typically, external side effects (such as launching tasks, sending responses, etc.)
        /// are suppressed during replay.
        /// </summary>
        public bool IsReplaying { get; set; }

        /// <summary>
        /// Applies the event to the given tracked object, using dynamic dispatch to 
        /// select the correct Process method overload for the event. 
        /// </summary>
        /// <param name="trackedObject"></param>
        /// <remarks>Called by the storage layer when this object calls applyToStore.</remarks>
        public void ProcessEffectOn(dynamic trackedObject)
        {
            trackedObject.Process(this.Effect, this);
        }

        public async Task ProcessUpdate(PartitionUpdateEvent updateEvent)
        {
            (long commitLogPosition, long inputQueuePosition) = this.getPositions();
            double startedTimestamp = this.Partition.CurrentTimeMs;

            using (EventTraceContext.MakeContext(commitLogPosition, updateEvent.EventIdString))
            {
                try
                {
                    this.Partition.EventDetailTracer?.TraceEventProcessingStarted(commitLogPosition, updateEvent, this.IsReplaying);

                    this.Effect = updateEvent;

                    // collect the initial list of targets
                    updateEvent.DetermineEffects(this);

                    // process until there are no more targets
                    while (this.Count > 0)
                    {
                        await ProcessRecursively().ConfigureAwait(false);
                    }

                    async ValueTask ProcessRecursively()
                    {
                        var startPos = this.Count - 1;
                        var key = this[startPos];

                        this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Process on [{key}]");

                        // start with processing the event on this object 
                        await this.applyToStore(key, this).ConfigureAwait(false);

                        // recursively process all additional objects to process
                        while (this.Count - 1 > startPos)
                        {
                            await ProcessRecursively().ConfigureAwait(false);
                        }

                        // pop this object now since we are done processing
                        this.RemoveAt(startPos);
                    }

                    this.Effect = null;
                }
                catch (OperationCanceledException)
                {
                    // o.k. during termination
                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    // for robustness, swallow exceptions, but report them
                    this.Partition.ErrorHandler.HandleError(nameof(ProcessUpdate), $"Encountered exception while processing update event {updateEvent}", exception, false, false);
                }
                finally
                {
                    double finishedTimestamp = this.Partition.CurrentTimeMs;
                    this.Partition.EventTraceHelper.TraceEventProcessed(commitLogPosition, updateEvent, startedTimestamp, finishedTimestamp, this.IsReplaying);
                }
            }
        }

        public void ProcessReadResult(PartitionReadEvent readEvent, TrackedObjectKey key, TrackedObject target)
        {
            if (readEvent == null)
            {
                // this read is not caused by a read event but was issued directly
                // in that case we are not processing the result here
                return;
            }

            (long commitLogPosition, long inputQueuePosition) = this.getPositions();
            this.Partition.Assert(!this.IsReplaying); // read events are never part of the replay
            double startedTimestamp = this.Partition.CurrentTimeMs;

            using (EventTraceContext.MakeContext(commitLogPosition, readEvent.EventIdString))
            {
                try
                {
                    readEvent.Deliver(key, target, out var isReady);

                    if (isReady)
                    {
                        this.Partition.EventDetailTracer?.TraceEventProcessingStarted(commitLogPosition, readEvent, false);

                        // trace read accesses to instance and history
                        switch (key.ObjectType)
                        {
                            case TrackedObjectKey.TrackedObjectType.Instance:
                                InstanceState instanceState = (InstanceState)target;
                                string instanceExecutionId = instanceState?.OrchestrationState?.OrchestrationInstance.ExecutionId;
                                string status = instanceState?.OrchestrationState?.OrchestrationStatus.ToString() ?? "null";
                                this.Partition.EventTraceHelper.TraceFetchedInstanceStatus(readEvent, key.InstanceId, instanceExecutionId, status, startedTimestamp - readEvent.IssuedTimestamp);
                                break;

                            case TrackedObjectKey.TrackedObjectType.History:
                                HistoryState historyState = (HistoryState)target;
                                string historyExecutionId = historyState?.ExecutionId;
                                int eventCount = historyState?.History?.Count ?? 0;
                                int episode = historyState?.Episode ?? 0;
                                this.Partition.EventTraceHelper.TraceFetchedInstanceHistory(readEvent, key.InstanceId, historyExecutionId, eventCount, episode, startedTimestamp - readEvent.IssuedTimestamp);
                                break;

                            default:
                                break;
                        }

                        readEvent.Fire(this.Partition);
                    }
                }
                catch (OperationCanceledException)
                {
                    // o.k. during termination
                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    // for robustness, swallow exceptions, but report them
                    this.Partition.ErrorHandler.HandleError(nameof(ProcessReadResult), $"Encountered exception while processing read event {readEvent}", exception, false, false);
                }
                finally
                {
                    double finishedTimestamp = this.Partition.CurrentTimeMs;
                    this.Partition.EventTraceHelper.TraceEventProcessed(commitLogPosition, readEvent, startedTimestamp, finishedTimestamp, false);
                }
            }
        }
        
        public async Task ProcessQueryResultAsync(PartitionQueryEvent queryEvent, IAsyncEnumerable<OrchestrationState> instances)
        {
            (long commitLogPosition, long inputQueuePosition) = this.getPositions();
            this.Partition.Assert(!this.IsReplaying); // query events are never part of the replay
            double startedTimestamp = this.Partition.CurrentTimeMs;

            using (EventTraceContext.MakeContext(commitLogPosition, queryEvent.EventIdString))
            {
                try
                {
                    this.Partition.EventDetailTracer?.TraceEventProcessingStarted(commitLogPosition, queryEvent, false);
                    await queryEvent.OnQueryCompleteAsync(instances, this.Partition);
                }
                catch (OperationCanceledException)
                {
                    // o.k. during termination
                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    // for robustness, swallow exceptions, but report them
                    this.Partition.ErrorHandler.HandleError(nameof(ProcessQueryResultAsync), $"Encountered exception while processing query event {queryEvent}", exception, false, false);
                }
                finally
                {
                    double finishedTimestamp = this.Partition.CurrentTimeMs;
                    this.Partition.EventTraceHelper.TraceEventProcessed(commitLogPosition, queryEvent, startedTimestamp, finishedTimestamp, false);
                }
            }
        }
    }
}
