// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    class ActivityOffloadReceived : PartitionMessageEvent
    {
        [DataMember]
        public List<(TaskMessage,string)> OffloadedActivities { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionToPartitionEventId(OffloadDecision.GetWorkItemId(this.OriginPartition, this.Timestamp), this.PartitionId);

        [IgnoreDataMember]
        public override IEnumerable<(TaskMessage,string)> TracedTaskMessages => this.OffloadedActivities;
    }
}