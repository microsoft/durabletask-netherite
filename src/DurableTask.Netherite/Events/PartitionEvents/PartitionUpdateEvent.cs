// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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

        [IgnoreDataMember]
        public virtual IEnumerable<(TaskMessage message, string workItemId)> TracedTaskMessages => PartitionUpdateEvent.noTaskMessages;

        static readonly IEnumerable<(TaskMessage, string)> noTaskMessages = Enumerable.Empty<(TaskMessage, string)>();

        public abstract void DetermineEffects(EffectTracker effects);
    }
}