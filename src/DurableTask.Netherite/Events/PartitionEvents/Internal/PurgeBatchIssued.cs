﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading.Tasks;

    [DataContract]
    class PurgeBatchIssued : PartitionUpdateEvent, IRequiresPrefetch
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

        public IEnumerable<TrackedObjectKey> KeysToPrefetch
        {
            get
            {
                foreach (var instanceId in this.InstanceIds)
                {
                    yield return TrackedObjectKey.Instance(instanceId);
                    yield return TrackedObjectKey.History(instanceId);
                }
            }
        }

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
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