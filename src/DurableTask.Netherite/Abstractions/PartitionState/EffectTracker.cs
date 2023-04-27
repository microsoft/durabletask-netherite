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
    abstract class EffectTracker : List<TrackedObjectKey>
    {
        readonly HashSet<TrackedObjectKey> deletedKeys;

        PartitionUpdateEvent currentUpdate;

        public EffectTracker()
        {
            this.deletedKeys = new HashSet<TrackedObjectKey>();
        }

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
            this.currentUpdate = updateEvent;
        }


        #region abstract methods

        public abstract ValueTask ApplyToStore(TrackedObjectKey key, EffectTracker tracker);

        public abstract ValueTask RemoveFromStore(IEnumerable<TrackedObjectKey> keys);
        
        public abstract (long, long) GetPositions();

        public abstract Partition Partition { get; }

        public abstract EventTraceHelper EventTraceHelper { get; }    

        public abstract EventTraceHelper EventDetailTracer { get; }

        protected abstract void HandleError(string where, string message, Exception e, bool terminatePartition, bool reportAsWarning);

        public abstract void Assert(bool condition, string message);

        public abstract uint PartitionId { get; }

        public abstract double CurrentTimeMs { get; }

        #endregion

        /// <summary>
        /// Applies the event to the given tracked object, using visitor pattern to
        /// select the correct Process method overload for the event. 
        /// </summary>
        /// <param name="trackedObject">The tracked object on which the event should be applied.</param>
        /// <remarks>Called by the storage layer when this object calls applyToStore.</remarks>
        public void ProcessEffectOn(TrackedObject trackedObject)
        {
            try
            {
                if (trackedObject.LastUpdate < this.currentUpdate.NextCommitLogPosition)
                {
                    this.currentUpdate.ApplyTo(trackedObject, this);
                    trackedObject.Version++;
                    trackedObject.LastUpdate = this.currentUpdate.NextCommitLogPosition;
                }
            }
            catch (Exception exception)
            {
                if (!Utils.IsFatal(exception))
                {
                    // for robustness, we swallow non-fatal exceptions inside event processing
                    // It does not mean they are not serious. We still report them as errors.
                    // (an incorrectly functioning partition is still better than a permanently dead one)
                    this.HandleError(nameof(ProcessEffectOn), $"Encountered exception on {trackedObject} when applying update event {this.currentUpdate} eventId={this.currentUpdate?.EventId}", exception, false, false);
                }
                else
                {
                    // since fatal exeptions are transient, we terminate the partition immediately, so the next incarnation can continue correctly
                    this.HandleError(nameof(ProcessEffectOn), $"Encountered fatal exception while applying update event eventId={this.currentUpdate?.EventId}", exception, true, false);
                }
            }
        }

        public void AddDeletion(TrackedObjectKey key)
        {
            this.deletedKeys.Add(key);
        }

        public async Task ProcessUpdate(PartitionUpdateEvent updateEvent)
        {
            long commitLogPosition = updateEvent.NextCommitLogPosition;
            double startedTimestamp = this.CurrentTimeMs;

            using (EventTraceContext.MakeContext(commitLogPosition, updateEvent.EventIdString))
            {
                try
                {
                    this.EventDetailTracer?.TraceEventProcessingStarted(commitLogPosition, updateEvent, EventTraceHelper.EventCategory.UpdateEvent, this.IsReplaying);

                    this.Assert(updateEvent != null, "null event in ProcessUpdate");

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

                        this.EventDetailTracer?.TraceEventProcessingDetail($"Process on [{key}]");

                        // start with processing the event on this object 
                        await this.ApplyToStore(key, this).ConfigureAwait(false);

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
                        await this.RemoveFromStore(this.deletedKeys);
                        this.deletedKeys.Clear();
                    }

                    this.SetCurrentUpdateEvent(null);
                }
                catch (OperationCanceledException)
                {
                    // o.k. during termination
                }
                catch (Exception exception)
                {
                    if (!Utils.IsFatal(exception))
                    {
                        // for robustness, we swallow non-fatal exceptions inside event processing
                        // It does not mean they are not serious. We still report them as errors.
                        // (an incorrectly functioning partition is still better than a permanently dead one)
                        this.HandleError(nameof(ProcessUpdate), $"Encountered exception while processing update event {updateEvent} eventId={updateEvent?.EventId}", exception, false, false);
                    }
                    else
                    {
                        // since fatal exeptions are transient, we terminate the partition immediately, so the next incarnation can continue correctly
                        this.HandleError(nameof(ProcessUpdate), $"Encountered fatal exception while processing update event eventId={this.currentUpdate?.EventId}", exception, true, false);
                    }
                }
                finally
                {
                    double finishedTimestamp = this.CurrentTimeMs;
                    this.EventTraceHelper?.TraceEventProcessed(commitLogPosition, updateEvent, EventTraceHelper.EventCategory.UpdateEvent, startedTimestamp, finishedTimestamp, this.IsReplaying);
                }
            }
        }

        public void ProcessReadResult(PartitionReadEvent readEvent, TrackedObjectKey key, TrackedObject target)
        {
            (long commitLogPosition, long inputQueuePosition) = this.GetPositions();
            this.Assert(!this.IsReplaying, "read events are not part of the replay");
            double startedTimestamp = this.CurrentTimeMs;

            using (EventTraceContext.MakeContext(commitLogPosition, readEvent.EventIdString))
            {
                try
                {
                    readEvent.Deliver(key, target, out var isReady);

                    // trace read accesses to instance and history
                    switch (key.ObjectType)
                    {
                        case TrackedObjectKey.TrackedObjectType.Instance:
                            InstanceState instanceState = (InstanceState)target;
                            string instanceExecutionId = instanceState?.OrchestrationState?.OrchestrationInstance.ExecutionId;
                            string status = instanceState?.OrchestrationState?.OrchestrationStatus.ToString() ?? "null";
                            this.EventTraceHelper?.TraceFetchedInstanceStatus(readEvent, key.InstanceId, instanceExecutionId, status, startedTimestamp - readEvent.IssuedTimestamp);
                            break;

                        case TrackedObjectKey.TrackedObjectType.History:
                            HistoryState historyState = (HistoryState)target;
                            string historyExecutionId = historyState?.ExecutionId;
                            int eventCount = historyState?.History?.Count ?? 0;
                            int episode = historyState?.Episode ?? 0;
                            this.EventTraceHelper?.TraceFetchedInstanceHistory(readEvent, key.InstanceId, historyExecutionId, eventCount, episode, historyState?.HistorySize ?? 0, startedTimestamp - readEvent.IssuedTimestamp);
                            break;

                        default:
                            break;
                    }

                    this.EventDetailTracer?.TraceEventProcessingStarted(commitLogPosition, readEvent, EventTraceHelper.EventCategory.ReadEvent, false);
                    
                    if (isReady)
                    {
                        readEvent.Fire(this.Partition);
                    }
                }
                catch (OperationCanceledException)
                {
                    // o.k. during termination
                }
                catch (Exception exception)
                {
                    if (!Utils.IsFatal(exception))
                    {
                        // for robustness, we swallow non-fatal exceptions inside event processing
                        // It does not mean they are not serious. We still report them as errors.
                        this.HandleError(nameof(ProcessReadResult), $"Encountered exception while processing read event {readEvent} eventId={readEvent?.EventId}", exception, false, false);
                    }
                    else
                    {
                        // since fatal exeptions are transient, we terminate the partition immediately, so the next incarnation can continue correctly
                        this.HandleError(nameof(ProcessReadResult), $"Encountered fatal exception while processing read event eventId={readEvent?.EventId}", exception, true, false);
                    }
                }
                finally
                {
                    double finishedTimestamp = this.CurrentTimeMs;
                    this.EventTraceHelper?.TraceEventProcessed(commitLogPosition, readEvent, EventTraceHelper.EventCategory.ReadEvent, startedTimestamp, finishedTimestamp, false);
                }
            }
        }
        
        public async Task ProcessQueryResultAsync(PartitionQueryEvent queryEvent, IAsyncEnumerable<(string, OrchestrationState)> instances, DateTime attempt)
        {
            (long commitLogPosition, long inputQueuePosition) = this.GetPositions();
            this.Assert(!this.IsReplaying, "query events are never part of the replay");
            double startedTimestamp = this.CurrentTimeMs;

            using (EventTraceContext.MakeContext(commitLogPosition, queryEvent.EventIdString))
            {
                try
                {
                    this.EventDetailTracer?.TraceEventProcessingStarted(commitLogPosition, queryEvent, EventTraceHelper.EventCategory.QueryEvent, false);
                    await queryEvent.OnQueryCompleteAsync(instances, this.Partition, attempt);
                }
                catch (OperationCanceledException)
                {
                    // o.k. during termination
                }
                catch (Exception exception)
                {
                    if (!Utils.IsFatal(exception))
                    {
                        // for robustness, we swallow non-fatal exceptions inside event processing
                        // It does not mean they are not serious. We still report them as errors.
                        this.HandleError(nameof(ProcessQueryResultAsync), $"Encountered exception while processing query event {queryEvent} eventId={queryEvent?.EventId}", exception, false, false);
                    }
                    else
                    {
                        // since fatal exeptions are transient, we terminate the partition immediately, so the next incarnation can continue correctly
                        this.HandleError(nameof(ProcessQueryResultAsync), $"Encountered fatal exception while processing query event eventId={queryEvent?.EventId}", exception, true, false);
                    }
                }
                finally
                {
                    double finishedTimestamp = this.CurrentTimeMs;
                    this.EventTraceHelper?.TraceEventProcessed(commitLogPosition, queryEvent, EventTraceHelper.EventCategory.QueryEvent, startedTimestamp, finishedTimestamp, false);
                }
            }
        }
    }
}
