﻿// Copyright (c) Microsoft Corporation.
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
        public List<WaitRequestReceived> Waiters { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Instance, this.InstanceId);

        public override string ToString()
        {
            return $"Instance InstanceId={this.InstanceId} Status={this.OrchestrationState?.OrchestrationStatus}";
        }

        public override void Process(CreationRequestReceived creationRequestReceived, EffectTracker effects)
        {
            bool filterDuplicate = this.OrchestrationState != null
                && creationRequestReceived.DedupeStatuses != null
                && creationRequestReceived.DedupeStatuses.Contains(this.OrchestrationState.OrchestrationStatus);

            if (!filterDuplicate)
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

                // queue the message in the session, or start a timer if delayed
                if (!ee.ScheduledStartTime.HasValue)
                {
                    effects.Add(TrackedObjectKey.Sessions);
                }
                else
                {
                    effects.Add(TrackedObjectKey.Timers);
                }
            }

            effects.Add(TrackedObjectKey.Outbox);
            creationRequestReceived.ResponseToSend = new CreationResponseReceived()
            {
                ClientId = creationRequestReceived.ClientId,
                RequestId = creationRequestReceived.RequestId,
                Succeeded = !filterDuplicate,
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
            // update the current orchestration state based on the new events
            this.OrchestrationState = UpdateOrchestrationState(this.OrchestrationState, evt.NewEvents);

            if (evt.CustomStatusUpdated)
            {
                this.OrchestrationState.Status = evt.CustomStatus;
            }

            this.OrchestrationState.LastUpdatedTime = evt.Timestamp;

            if (evt.State != null)
            {
                this.Partition.Assert(
                (
                    evt.State.Version,
                    evt.State.Status,
                    evt.State.Output,
                    evt.State.Name,
                    evt.State.Input,
                    evt.State.OrchestrationInstance.InstanceId,
                    evt.State.OrchestrationInstance.ExecutionId,
                    evt.State.CompletedTime,
                    evt.State.OrchestrationStatus,
                    evt.State.LastUpdatedTime,
                    evt.State.CreatedTime,
                    evt.State.ScheduledStartTime,
                    evt.State.OrchestrationInstance.ExecutionId
                )
                ==
                (
                    this.OrchestrationState.Version,
                    this.OrchestrationState.Status,
                    this.OrchestrationState.Output,
                    this.OrchestrationState.Name,
                    this.OrchestrationState.Input,
                    this.OrchestrationState.OrchestrationInstance.InstanceId,
                    this.OrchestrationState.OrchestrationInstance.ExecutionId,
                    this.OrchestrationState.CompletedTime,
                    this.OrchestrationState.OrchestrationStatus,
                    this.OrchestrationState.LastUpdatedTime,
                    this.OrchestrationState.CreatedTime,
                    this.OrchestrationState.ScheduledStartTime,
                    evt.ExecutionId
                ));
            }
            
            // if the orchestration is complete, notify clients that are waiting for it
            if (this.Waiters != null)
            {
                if (WaitRequestReceived.SatisfiesWaitCondition(this.OrchestrationState))
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
            if (orchestrationState == null)
            {
                orchestrationState = new OrchestrationState();
            }
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
            if (WaitRequestReceived.SatisfiesWaitCondition(this.OrchestrationState))
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

        public override void Process(DeletionRequestReceived deletionRequest, EffectTracker effects)
        {
            int numberInstancesDeleted = 0;

            if (this.OrchestrationState != null
                && (!deletionRequest.CreatedTime.HasValue || deletionRequest.CreatedTime.Value == this.OrchestrationState.CreatedTime))
            {
                numberInstancesDeleted++;

                // delete instance object and history object
                effects.AddDeletion(this.Key);
                effects.AddDeletion(TrackedObjectKey.History(this.InstanceId));

                // also delete all task messages headed for this instance
                effects.Add(TrackedObjectKey.Sessions);
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
            if (this.OrchestrationState != null
                && purgeBatchIssued.InstanceQuery.Matches(this.OrchestrationState))
            {
                purgeBatchIssued.Purged.Add(this.InstanceId);

                // delete instance object and history object
                effects.AddDeletion(this.Key);
                effects.AddDeletion(TrackedObjectKey.History(this.InstanceId));
            }
        }
    }
}