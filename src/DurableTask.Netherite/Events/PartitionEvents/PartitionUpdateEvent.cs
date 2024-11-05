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

    [DataContract]
    abstract class PartitionUpdateEvent : PartitionEvent
    {
        /// <summary>
        /// The position of the next event after this one. For read-only events, zero.
        /// </summary>
        /// <remarks>We do not persist this in the log since it is implicit, nor transmit this in packets since it has only local meaning.</remarks>
        [IgnoreDataMember]
        public long NextCommitLogPosition { get; set; }

        [IgnoreDataMember]
        public OutboxState.Batch OutboxBatch { get; set; }

        /// <summary>
        /// Used to determine which partition state objects should be modified by an event.
        /// </summary>
        public abstract void DetermineEffects(EffectTracker effects);

        public abstract void ApplyTo(TrackedObject trackedObject, EffectTracker effectTracker);
    }
}