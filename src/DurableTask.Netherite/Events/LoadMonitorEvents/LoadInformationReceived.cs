// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;

    [DataContract]
    class LoadInformationReceived : LoadMonitorEvent
    {
        [DataMember]
        public uint PartitionId { get; set; } // The partition that sent the load information

        [DataMember]
        public int Stationary { get; set; } // The number of queued activities that can only execute on this partition

        [DataMember]
        public int Mobile { get; set; } // The number of queued activities that are available for offloading

        [DataMember]
        public double? AverageActCompletionTime { get; set; }

        [DataMember]
        public Dictionary<uint, DateTime> OffloadsReceived { get; set; }

        public bool ConfirmsSource(OffloadCommandReceived cmd)
        {
            uint source = cmd.PartitionId;
            DateTime id = cmd.Timestamp;

            return
                source == this.PartitionId
                && this.OffloadsReceived != null
                && this.OffloadsReceived.TryGetValue(source, out DateTime lastReceived)
                && lastReceived >= id;
        }

        public bool ConfirmsDestination(OffloadCommandReceived cmd)
        {
            uint source = cmd.PartitionId;
            uint destination = cmd.OffloadDestination;
            DateTime id = cmd.Timestamp;

            return
                destination == this.PartitionId
                && this.OffloadsReceived != null
                && this.OffloadsReceived.TryGetValue(source, out DateTime lastReceived)
                && lastReceived >= id;
        }
    }
}
