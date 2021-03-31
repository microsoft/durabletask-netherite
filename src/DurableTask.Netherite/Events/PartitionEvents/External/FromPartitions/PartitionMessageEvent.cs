// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Runtime.Serialization;

    [DataContract]
    abstract class PartitionMessageEvent : PartitionUpdateEvent
    {
        [DataMember]
        public uint OriginPartition { get; set; }

        [DataMember]
        public long OriginPosition { get; set; }

        [IgnoreDataMember]
        public virtual (long, int) DedupPosition => (this.OriginPosition, 0); // overridden if a subposition is needed

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Dedup);
        }
    }
}