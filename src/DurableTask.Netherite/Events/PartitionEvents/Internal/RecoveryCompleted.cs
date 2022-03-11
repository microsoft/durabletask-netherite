// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
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
        public string InputQueueFingerprint { get; set; }

        [DataMember]
        public bool ResendAll { get; set; }

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(" RecoveredPosition=");
            s.Append(this.RecoveredPosition);
            s.Append(" ResendAll=");
            s.Append(this.ResendAll);
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
            effects.Add(TrackedObjectKey.Outbox);
            effects.Add(TrackedObjectKey.Timers);
            effects.Add(TrackedObjectKey.Prefetch);
            effects.Add(TrackedObjectKey.Queries);
        }
    }
}
