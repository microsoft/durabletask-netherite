// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    abstract class PartitionMessageEvent : PartitionUpdateEvent
    {
        [DataMember]
        public uint OriginPartition { get; set; }

        [DataMember]
        public long OriginPosition { get; set; }

        [IgnoreDataMember]
        public virtual (long, int) DedupPosition => (this.OriginPosition, 0); // overridden if a subposition is needed

        [IgnoreDataMember]
        public abstract IEnumerable<(TaskMessage message, string workItemId)> TracedTaskMessages { get; }
      
        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Dedup);
        }

        public bool ConfirmedBy(AcksReceived evt)
        {
            (long, int)? reported = evt.ReceivePositions[(int)this.PartitionId];
            return reported.HasValue && reported.Value.CompareTo(this.DedupPosition) >= 0;
        }
    }
}