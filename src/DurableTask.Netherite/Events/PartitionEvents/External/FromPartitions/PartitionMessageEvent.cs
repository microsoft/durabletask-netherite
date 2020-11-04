// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Dedup);
        }
    }
}