// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    class ActivityTransferReceived : PartitionMessageEvent
    {
        [DataMember]
        public List<(TaskMessage message, string workItemId)> TransferredActivities { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }


        [IgnoreDataMember]
        public override EventId EventId => 
            EventId.MakePartitionToPartitionEventId(
                ActivityScheduling.GetWorkItemId(this.OriginPartition, this.Timestamp), this.PartitionId);

        [IgnoreDataMember]
        public override IEnumerable<(TaskMessage message, string workItemId)> TracedTaskMessages =>
            this.TransferredActivities;
    }
}