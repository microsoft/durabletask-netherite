// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;

    [DataContract]
    class InstanceState : TrackedObject
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public OrchestrationState OrchestrationState { get; set; }

        [DataMember]
        public long OrchestrationStateSize { get; set; }

        [DataMember]
        public List<WaitRequestReceived> Waiters { get; set; }

        [DataMember]
        public string CreationRequestEventId { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Instance, this.InstanceId);

        public override string ToString()
        {
            return $"Instance InstanceId={this.InstanceId} Status={this.OrchestrationState?.OrchestrationStatus}";
        }

        public override long EstimatedSize => 60 + 2 * (this.InstanceId?.Length ?? 0) + this.OrchestrationStateSize;

        public override void Process(CreationRequestReceived creationRequestReceived, EffectTracker effects)
        {
            if (creationRequestReceived.EventIdString == this.CreationRequestEventId)
            {
                // we have already processed this event - it must be a duplicate delivery. Ignore it.
                return;
            };

            bool exists = this.OrchestrationState != null;

            bool previousExecutionWithDedupeStatus = exists
                && creationRequestReceived.DedupeStatuses != null
                && creationRequestReceived.DedupeStatuses.Contains(this.OrchestrationState.OrchestrationStatus);

            if (!previousExecutionWithDedupeStatus)
            {
                var ee = creationRequestReceived.ExecutionStartedEvent;

                // set the orchestration state now (before processing the creation in the history)
                // so that this new instance is "on record" immediately - it is guaranteed to replace whatever is in flight
                this.OrchestrationState = new OrchestrationState
                {
                    Name = ee.Name,
                    Version = ee.Version,
                    OrchestrationInstance = ee.OrchestrationInstance,
                    OrchestrationStatus = OrchestrationStatus.Pending,
                    ParentInstance = ee.ParentInstance,
                    Input = ee.Input,
                    Tags = ee.Tags,
                    CreatedTime = ee.Timestamp,
                    LastUpdatedTime = ee.Timestamp,
                    CompletedTime = Core.Common.DateTimeUtils.MinDateTime,
                    ScheduledStartTime = ee.ScheduledStartTime
                };
                this.OrchestrationStateSize = DurableTask.Netherite.SizeUtils.GetEstimatedSize(this.OrchestrationState);
                this.CreationRequestEventId = creationRequestReceived.EventIdString;

                // queue the message in the session, or start a timer if delayed
                if (!ee.ScheduledStartTime.HasValue)
                {
                    effects.Add(TrackedObjectKey.Sessions);
                }
                else
                {
                    effects.Add(TrackedObjectKey.Timers);
                }

                if (!exists)
                {
                    effects.Add(TrackedObjectKey.Stats);
                }
            }

            effects.Add(TrackedObjectKey.Outbox);
            creationRequestReceived.ResponseToSend = new CreationResponseReceived()
            {
                ClientId = creationRequestReceived.ClientId,
                RequestId = creationRequestReceived.RequestId,
                Succeeded = !previousExecutionWithDedupeStatus,
                ExistingInstanceOrchestrationStatus = this.OrchestrationState?.OrchestrationStatus,
            };
        }

        void CullWaiters(DateTime threshold)
        {
            // remove all waiters whose timeout is before the threshold
            if (this.Waiters.Any(request => request.TimeoutUtc <= threshold))
            {
                this.Waiters = this.Waiters
                            .Where(request => request.TimeoutUtc > threshold)
                            .ToList();
            }
        }

        public override void Process(BatchProcessed evt, EffectTracker effects)
        {
            if (evt.DeleteInstance)
            {
                // this instance is an entity that was implicitly deleted
                if (this.OrchestrationState != null)
                {
                    // decrement the instance count
                    effects.Add(TrackedObjectKey.Stats);
                }
                this.OrchestrationState = null;
                this.OrchestrationStateSize = 0;
                effects.AddDeletion(this.Key);
                return;
            }

            if (this.OrchestrationState == null)
            {
                // a new instance is created (this can happen for suborchestrations or for entities)
                this.OrchestrationState = new OrchestrationState();
                effects.Add(TrackedObjectKey.Stats);
            }

            // update the current orchestration state based on the new events
            this.OrchestrationState = UpdateOrchestrationState(this.OrchestrationState, evt.NewEvents);

            if (evt.CustomStatusUpdated)
            {
                this.OrchestrationState.Status = evt.CustomStatus;
            }

            this.OrchestrationState.LastUpdatedTime = evt.Timestamp;
            this.OrchestrationStateSize = DurableTask.Netherite.SizeUtils.GetEstimatedSize(this.OrchestrationState);

            // if the orchestration is complete, notify clients that are waiting for it
            if (this.Waiters != null)
            {
                if (WaitRequestReceived.SatisfiesWaitCondition(this.OrchestrationState?.OrchestrationStatus))
                {
                    // we do not need effects.Add(TrackedObjectKey.Outbox) because it has already been added by SessionsState
                    evt.ResponsesToSend = this.Waiters.Select(request => request.CreateResponse(this.OrchestrationState)).ToList();

                    this.Waiters = null;
                }
                else
                {
                    this.CullWaiters(evt.Timestamp);
                }
            }
        }

        static OrchestrationState UpdateOrchestrationState(OrchestrationState orchestrationState, List<HistoryEvent> events)
        {
            foreach (var evt in events)
            {
                if (evt is ExecutionStartedEvent executionStartedEvent)
                {
                    orchestrationState.OrchestrationInstance = executionStartedEvent.OrchestrationInstance;
                    orchestrationState.CreatedTime = executionStartedEvent.Timestamp;
                    orchestrationState.Input = executionStartedEvent.Input;
                    orchestrationState.Name = executionStartedEvent.Name;
                    orchestrationState.Version = executionStartedEvent.Version;
                    orchestrationState.Tags = executionStartedEvent.Tags;
                    orchestrationState.ParentInstance = executionStartedEvent.ParentInstance;
                    orchestrationState.ScheduledStartTime = executionStartedEvent.ScheduledStartTime;
                    orchestrationState.CompletedTime = Core.Common.Utils.DateTimeSafeMaxValue;
                    orchestrationState.Output = null;
                    orchestrationState.OrchestrationStatus = OrchestrationStatus.Running;
                }
                else if (evt is ExecutionCompletedEvent executionCompletedEvent)
                {
                    orchestrationState.CompletedTime = executionCompletedEvent.Timestamp;
                    orchestrationState.Output = executionCompletedEvent.Result;
                    orchestrationState.OrchestrationStatus = executionCompletedEvent.OrchestrationStatus;
                }
            }
            return orchestrationState;
        }

        public override void Process(WaitRequestReceived evt, EffectTracker effects)
        {
            if (WaitRequestReceived.SatisfiesWaitCondition(this.OrchestrationState?.OrchestrationStatus))
            {
                effects.Add(TrackedObjectKey.Outbox);
                evt.ResponseToSend =  evt.CreateResponse(this.OrchestrationState);              
            }
            else
            {
                if (this.Waiters == null)
                {
                    this.Waiters = new List<WaitRequestReceived>();
                }
                else
                {
                    this.CullWaiters(evt.Timestamp);
                }
                
                this.Waiters.Add(evt);
            }
        }

        void DeleteState(EffectTracker effects)
        {
            // delete instance object and history object
            effects.AddDeletion(this.Key);
            effects.AddDeletion(TrackedObjectKey.History(this.InstanceId));

            this.OrchestrationState = null;
            this.OrchestrationStateSize = 0;
            this.CreationRequestEventId = null;
            this.Waiters = null;
        }

        public override void Process(DeletionRequestReceived deletionRequest, EffectTracker effects)
        {
            int numberInstancesDeleted = 0;

            if (this.OrchestrationState != null
                && (!deletionRequest.CreatedTime.HasValue || deletionRequest.CreatedTime.Value == this.OrchestrationState.CreatedTime))
            {
                numberInstancesDeleted++;

                this.DeleteState(effects);

                // also delete all task messages headed for this instance
                effects.Add(TrackedObjectKey.Sessions);

                // update the instance count
                effects.Add(TrackedObjectKey.Stats);
            }

            effects.Add(TrackedObjectKey.Outbox);
            deletionRequest.ResponseToSend = new DeletionResponseReceived()
            {
                ClientId = deletionRequest.ClientId,
                RequestId = deletionRequest.RequestId,
                NumberInstancesDeleted = numberInstancesDeleted,
            };
        }

        public override void Process(PurgeBatchIssued purgeBatchIssued, EffectTracker effects)
        {
            OrchestrationState state = this.OrchestrationState;
            bool matchesQuery = (state != null) && purgeBatchIssued.InstanceQuery.Matches(state);

            effects.EventDetailTracer?.TraceEventProcessingDetail($"status={state?.OrchestrationStatus} matchesQuery={matchesQuery}");

            if (matchesQuery)
            {
                purgeBatchIssued.Purged.Add(this.InstanceId);

                this.DeleteState(effects);
            }
        }
    }
}