// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Netherite.Faster;

    [DataContract]
    class DedupState : TrackedObject
    {
        [DataMember]
        public Dictionary<uint, (long Position, int SubPosition)> LastProcessed { get; set; } = new Dictionary<uint, (long,int)>();

        [DataMember]
        public (long, long) Positions; // used by FasterAlt to persist positions

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Dedup);

        bool IsNotDuplicate(PartitionMessageEvent evt)
        {
            // detect duplicates of incoming partition-to-partition events by comparing commit log position /subposition of this event against last processed event from same partition
            this.LastProcessed.TryGetValue(evt.OriginPartition, out (long,int) lastProcessed);

            if (evt.DedupPosition.CompareTo(lastProcessed) > 0)
            {
                this.LastProcessed[evt.OriginPartition] = evt.DedupPosition;
                return true;
            }
            else
            {
                return false;
            }
        }

        public void Process(TaskMessagesReceived evt, EffectTracker effects)
        {
            // contains messages to be processed by sessions and/or to be scheduled by timer
            if (this.IsNotDuplicate(evt))
            {
                if (evt.TaskMessages != null)
                {
                    effects.Add(TrackedObjectKey.Sessions);
                }
                if (evt.DelayedTaskMessages != null)
                {
                    effects.Add(TrackedObjectKey.Timers);
                }
            }
        }
    }
}
