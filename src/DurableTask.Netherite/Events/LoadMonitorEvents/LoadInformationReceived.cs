// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Runtime.Serialization;

    [DataContract]
    class LoadInformationReceived : LoadMonitorEvent
    {
        [DataMember]
        public uint PartitionId { get; set; } // The partition that sent the load information

        [DataMember]
        public int BacklogSize { get; set; } // TODO determine what metrics to use
    }
}
