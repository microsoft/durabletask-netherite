// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using DurableTask.Netherite.Scaling;

    [DataContract]
    class StatsState : TrackedObject
    {
        [DataMember]
        public long InstanceCount { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Stats);

        public override string ToString()
        {
            return $"Stats (InstanceCount={this.InstanceCount})";
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Instances = this.InstanceCount;
        }

        public override void Process(CreationRequestReceived evt, EffectTracker effects)
        {
            this.InstanceCount++;
        }

        public override void Process(BatchProcessed evt, EffectTracker effects)
        {
            this.InstanceCount++;
        }

        public override void Process(PurgeBatchIssued evt, EffectTracker effects)
        {
            this.InstanceCount -= evt.Purged.Count;
        }

        public override void Process(DeletionRequestReceived evt, EffectTracker effects)
        {
            this.InstanceCount--;
        }

    }
}
