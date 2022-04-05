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


        public bool DifferentFrom(PositionsReceived alt)
        {
            if (alt == null || this.PartitionId != alt.PartitionId)
            {
                return true;
            }
            for (int i = 0; i < this.NextNeededAck.Length; i++)
            {
                if (!this.NextNeededAck[i].Equals(alt.NextNeededAck[i]))
                {
                    return true;
                }
                if (!this.ReceivePositions[i].Equals(alt.ReceivePositions[i]))
                {
                    return true;
                }
            }
            return false;
        }
    }
}
