// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.EventHubs
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// The parameters for a specific taskhub instance.
    /// This is saved in the blob "taskhub-parameters.json".
    /// </summary>
    [DataContract]
    class TaskhubParameters
    {
        [DataMember]
        public string TaskhubName { get; set; }

        [DataMember]
        public Guid TaskhubGuid { get; set; }

        [DataMember]
        public DateTime CreationTimestamp { get; set; }

        [DataMember]
        public string[] PartitionHubs { get; set; }

        [DataMember]
        public string PartitionConsumerGroup { get; set; }

        [DataMember]
        public string[] ClientHubs { get; set; }

        [DataMember]
        public string ClientConsumerGroup { get; set; }

        [DataMember]
        public long[] StartPositions { get; set; }
    }

}
