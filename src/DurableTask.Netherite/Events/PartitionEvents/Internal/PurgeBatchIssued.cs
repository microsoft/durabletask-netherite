// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading.Tasks;

    [DataContract]
class PurgeBatchIssued : PartitionUpdateEvent
    {
        [DataMember]
        public string QueryEventId { get; set; }

        [DataMember]
        public int BatchNumber { get; set; }

        [DataMember]
        public List<string> InstanceIds { get; set; }

        [DataMember]
        public InstanceQuery InstanceQuery { get; set; }

        [IgnoreDataMember]
        public TaskCompletionSource<object> WhenProcessed { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(this.QueryEventId);

        [IgnoreDataMember]
        public List<string> Purged { get; set; }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(" batchNumber=");
            s.Append(this.BatchNumber);
            s.Append(" count=");
            s.Append(this.InstanceIds.Count);
        }

        public override void DetermineEffects(EffectTracker effects)
        {
            // the last-added effects are processed first
            // so they can set the Purged list to contain only the instance ids that are actually purged

            effects.Add(TrackedObjectKey.Queries);
            effects.Add(TrackedObjectKey.Sessions);

            this.Purged = new List<string>();
            foreach (string instanceId in this.InstanceIds)
            {
                effects.Add(TrackedObjectKey.Instance(instanceId));
            }
        }
    }
}