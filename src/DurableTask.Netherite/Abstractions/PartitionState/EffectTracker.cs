// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
        readonly Func<IEnumerable<TrackedObjectKey>, ValueTask> removeFromStore;
        readonly Func<(long, long)> getPositions;
        readonly System.Diagnostics.Stopwatch stopWatch;
        readonly HashSet<TrackedObjectKey> deletedKeys;

        public EffectTracker(Partition partition, Func<TrackedObjectKey, EffectTracker, ValueTask> applyToStore, Func<IEnumerable<TrackedObjectKey>, ValueTask> removeFromStore, Func<(long, long)> getPositions)
        {
            this.Partition = partition;
            this.applyToStore = applyToStore;
            this.removeFromStore = removeFromStore;
            this.getPositions = getPositions;
            this.stopWatch = new System.Diagnostics.Stopwatch();
            this.deletedKeys = new HashSet<TrackedObjectKey>();
            this.stopWatch.Start();
        }

        /// <summary>
        /// The current partition.
        /// </summary>
        public Partition Partition { get; }

        /// <summary>
        /// The event id of the current effect.
        /// </summary>
        public string CurrentEventId => this.currentUpdate?.EventIdString;

        /// <summary>
        /// True if we are replaying this effect during recovery.
        /// Typically, external side effects (such as launching tasks, sending responses, etc.)
        /// are suppressed during replay.
        /// </summary>
        public bool IsReplaying { get; set; }

        void SetCurrentUpdateEvent(PartitionUpdateEvent updateEvent)
        {
            this.effect = this.currentUpdate = updateEvent;
        }

        PartitionUpdateEvent currentUpdate;
        dynamic effect;

        /// <summary>
        /// Applies the event to the given tracked object, using dynamic dispatch to 
        /// select the correct Process method overload for the event. 
        /// </summary>
        /// <param name="trackedObject">The tracked object on which the event should be applied.</param>
        /// <remarks>Called by the storage layer when this object calls applyToStore.</remarks>
        public void ProcessEffectOn(dynamic trackedObject)
        {
            this.Partition.Assert(this.currentUpdate != null);
            try
            {
                trackedObject.Process(this.effect, this);
            }
            catch (Exception exception) when (!Utils.IsFatal(exception))
            {
                // for robustness, we swallow exceptions inside event processing.
                // It does not mean they are not serious. We still report them as errors.
                this.Partition.ErrorHandler.HandleError(nameof(ProcessUpdate), $"Encountered exception on {trackedObject} when applying update event {this.currentUpdate}, eventId={this.currentUpdate?.EventId}", exception, false, false);
            }
        }

        public void AddDeletion(TrackedObjectKey key)
        {
            this.deletedKeys.Add(key);
        }

        public async Task ProcessUpdate(PartitionUpdateEvent updateEvent)
        {
            (long commitLogPosition, long inputQueuePosition) = this.getPositions();
            double startedTimestamp = this.Partition.CurrentTimeMs;

            using (EventTraceContext.MakeContext(commitLogPosition, updateEvent.EventIdString))
            {
                try
                {
                    this.Partition.EventDetailTracer?.TraceEventProcessingStarted(commitLogPosition, updateEvent, EventTraceHelper.EventCategory.UpdateEvent, this.IsReplaying);

                    this.Partition.Assert(updateEvent != null);

                    this.SetCurrentUpdateEvent(updateEvent);

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

                    if (this.deletedKeys.Count > 0)
                    {
                        await this.removeFromStore(this.deletedKeys);
                        this.deletedKeys.Clear();
                    }

                    this.SetCurrentUpdateEvent(null);
                }
                catch (OperationCanceledException)
                {
                    // o.k. during termination
                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    // for robustness, we swallow exceptions inside event processing.
                    // It does not mean they are not serious. We still report them as errors.
                    this.Partition.ErrorHandler.HandleError(nameof(ProcessUpdate), $"Encountered exception while processing update event {updateEvent}", exception, false, false);
                }
                finally
                {
                    double finishedTimestamp = this.Partition.CurrentTimeMs;
                    this.Partition.EventTraceHelper.TraceEventProcessed(commitLogPosition, updateEvent, EventTraceHelper.EventCategory.UpdateEvent, startedTimestamp, finishedTimestamp, this.IsReplaying);
                }
            }
        }

        public void ProcessReadResult(PartitionReadEvent readEvent, TrackedObjectKey key, TrackedObject target)
        {
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
                        this.Partition.EventDetailTracer?.TraceEventProcessingStarted(commitLogPosition, readEvent, EventTraceHelper.EventCategory.ReadEvent, false);

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
                    // for robustness, we swallow exceptions inside event processing.
                    // It does not mean they are not serious. We still report them as errors.
                    this.Partition.ErrorHandler.HandleError(nameof(ProcessReadResult), $"Encountered exception while processing read event {readEvent}", exception, false, false);
                }
                finally
                {
                    double finishedTimestamp = this.Partition.CurrentTimeMs;
                    this.Partition.EventTraceHelper.TraceEventProcessed(commitLogPosition, readEvent, EventTraceHelper.EventCategory.ReadEvent, startedTimestamp, finishedTimestamp, false);
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
                    this.Partition.EventDetailTracer?.TraceEventProcessingStarted(commitLogPosition, queryEvent, EventTraceHelper.EventCategory.QueryEvent, false);
                    await queryEvent.OnQueryCompleteAsync(instances, this.Partition);
                }
                catch (OperationCanceledException)
                {
                    // o.k. during termination
                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    // for robustness, we swallow exceptions inside event processing.
                    // It does not mean they are not serious. We still report them as errors.
                    this.Partition.ErrorHandler.HandleError(nameof(ProcessQueryResultAsync), $"Encountered exception while processing query event {queryEvent}", exception, false, false);
                }
                finally
                {
                    double finishedTimestamp = this.Partition.CurrentTimeMs;
                    this.Partition.EventTraceHelper.TraceEventProcessed(commitLogPosition, queryEvent, EventTraceHelper.EventCategory.QueryEvent, startedTimestamp, finishedTimestamp, false);
                }
            }
        }
    }
}
