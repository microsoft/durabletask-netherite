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
        public int BacklogSize { get; set; } // TODO determine what metrics to use

        [DataMember]
        public double? AverageActCompletionTime { get; set; }

        [DataMember]
        public Dictionary<uint, DateTime> OffloadsReceived { get; set; }

        public bool ConfirmsReceiptOf(OffloadCommandReceived cmd)
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
