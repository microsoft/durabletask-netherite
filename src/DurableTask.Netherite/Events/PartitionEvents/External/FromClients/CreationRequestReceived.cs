// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core;
    using DurableTask.Core.History;

    [DataContract]
    class CreationRequestReceived : ClientRequestEventWithPrefetch
    {
        [DataMember]
        public OrchestrationStatus[] DedupeStatuses { get; set; }

        [DataMember]
        public TaskMessage TaskMessage { get; set; }
        
        [DataMember]
        public bool FilteredDuplicate { get; set; }

        [DataMember]
        public OrchestrationStatus? ExistingInstanceOrchestrationStatus { get; set; }

        [IgnoreDataMember]
        public ExecutionStartedEvent ExecutionStartedEvent => this.TaskMessage.Event as ExecutionStartedEvent;

        [IgnoreDataMember]
        public string InstanceId => this.ExecutionStartedEvent.OrchestrationInstance.InstanceId;
 
        [IgnoreDataMember]
        public override string TracedInstanceId => this.InstanceId;

        [IgnoreDataMember]
        public override TrackedObjectKey Target => TrackedObjectKey.Instance(this.InstanceId);

        [IgnoreDataMember]
        public CreationResponseReceived ResponseToSend { get; set; } // used to communicate response to ClientState

        [IgnoreDataMember]
        public DateTime CreationTimestamp { get; set; }

        public override bool OnReadComplete(TrackedObject target, Partition partition)
        {
            // Use this moment of time as the creation timestamp, which is going to replace the original timestamp taken on the client.
            // This is preferrable because it avoids clock synchronization issues (which can result in negative orchestration durations)
            // and means the timestamp is consistently ordered with respect to timestamps of other events on this partition.
            this.CreationTimestamp = DateTime.UtcNow;

            return true;
        }

        // make a copy of an event so we run it through the pipeline a second time
        public override PartitionEvent Clone()
        {
            var evt = (CreationRequestReceived)base.Clone();

            // make a copy of the execution started event in order to modify the creation timestamp

            var ee = this.ExecutionStartedEvent;
            var tm = this.TaskMessage;

            evt.TaskMessage = new TaskMessage()
            {
                // TODO: consider using Object.MemberwiseClone() to copy ExecutionStartedEvent
                Event = new ExecutionStartedEvent(ee.EventId, ee.Input)
                {
                    ParentInstance = ee.ParentInstance,
                    Name = ee.Name,
                    Version = ee.Version,
                    Tags = ee.Tags,
                    ScheduledStartTime = ee.ScheduledStartTime,
                    Correlation = ee.Correlation,
                    ExtensionData = ee.ExtensionData,
                    OrchestrationInstance = ee.OrchestrationInstance,
                    Timestamp = this.CreationTimestamp,
                    ParentTraceContext = ee.ParentTraceContext,
                },
                SequenceNumber = tm.SequenceNumber,
                OrchestrationInstance = tm.OrchestrationInstance,
                ExtensionData = tm.ExtensionData,
            };

            return evt;
        }

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }
    }
}
