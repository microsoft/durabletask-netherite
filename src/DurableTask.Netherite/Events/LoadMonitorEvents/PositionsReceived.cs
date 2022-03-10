// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using Dynamitey.DynamicObjects;

    [DataContract]
    class PositionsReceived : LoadMonitorEvent
    {
        [DataMember]
        public uint PartitionId { get; set; } // The partition that sent the load information

        [DataMember]
        public (long,int)?[] NextNeededAck { get; set; } // for each partition, the oldest needed ack, or null if none

        [DataMember]
        public (long,int)?[] ReceivePositions { get; set; } // for each partition, the most recently received event
    }
}
