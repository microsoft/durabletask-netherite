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
            EventId.MakePartitionToPartitionEventId($"{this.OriginPartition:D2}F{this.Timestamp:o}", this.PartitionId);

        [IgnoreDataMember]
        public override IEnumerable<(TaskMessage message, string workItemId)> TracedTaskMessages =>
            this.TransferredActivities;
    }
}