// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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