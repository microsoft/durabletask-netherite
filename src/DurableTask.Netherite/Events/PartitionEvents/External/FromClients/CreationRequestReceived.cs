﻿// Copyright (c) Microsoft Corporation.
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
        public DateTime Timestamp { get; set; }

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

        public override bool OnReadComplete(TrackedObject target, Partition partition)
        {
            // Use this moment of time as the creation timestamp, replacing the original timestamp taken on the client.
            // This is preferrable because it avoids clock synchronization issues (which can result in negative orchestration durations)
            // and means the timestamp is consistently ordered with respect to timestamps of other events on this partition.
            DateTime creationTimestamp = DateTime.UtcNow;

            this.ExecutionStartedEvent.Timestamp = creationTimestamp;

            return true;
        }

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }
    }
}