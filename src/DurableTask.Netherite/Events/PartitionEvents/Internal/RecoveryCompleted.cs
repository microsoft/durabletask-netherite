// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;

    [DataContract]
    class RecoveryCompleted : PartitionUpdateEvent
    {
        [DataMember]
        public long RecoveredPosition { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [DataMember]
        public string WorkerId { get; set; }

        [DataMember]
        public int NumActivities { get; set; }

        [DataMember]
        public int MaxActivityDequeueCount { get; set; }

        [DataMember]
        public int NumSessions { get; set; }

        [DataMember]
        public int MaxSessionDequeueCount { get; set; }

        [IgnoreDataMember]
        public bool RequiresStateUpdate => (this.NumSessions + this.NumActivities) > 0; // orchestrations and activities must increment the dequeue count

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(" RecoveredPosition=");
            s.Append(this.RecoveredPosition);
            s.Append(" NumActivities=");
            s.Append(this.NumActivities);
            s.Append(" MaxActivityDequeueCount=");
            s.Append(this.MaxActivityDequeueCount);
            s.Append(" NumSessions=");
            s.Append(this.NumSessions);
            s.Append(" MaxSessionDequeueCount=");
            s.Append(this.MaxSessionDequeueCount);
        }

        public override void OnSubmit(Partition partition)
        {
            partition.EventTraceHelper.TraceEventProcessingDetail($"Submitted {this}");
        }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId($"Recovered-{this.WorkerId}-{this.Timestamp:o}");

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Activities);
            effects.Add(TrackedObjectKey.Sessions);
        }
    }
}
