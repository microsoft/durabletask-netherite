﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;

    [DataContract]
    class RemoteActivityResultReceived : PartitionMessageEvent
    {
        [DataMember]
        public TaskMessage Result { get; set; }

        [DataMember]
        public long ActivityId { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [DataMember]
        public double LatencyMs { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionToPartitionEventId(this.WorkItemId, this.PartitionId);

        [IgnoreDataMember]
        public string WorkItemId => ActivitiesState.GetWorkItemId(this.OriginPartition, this.ActivityId);

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

        [IgnoreDataMember]
        public override IEnumerable<(TaskMessage message, string workItemId)> TracedTaskMessages
        {
            get
            {
                yield return (this.Result, this.WorkItemId);
            }
        }

    }
}