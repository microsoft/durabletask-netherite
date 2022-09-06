// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Abstractions
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// The parameters for a specific taskhub instance.
    /// This is used for the taskhubparameters.json file. 
    /// </summary>
    [DataContract]
    public class TaskhubParameters
    {
        [DataMember]
        public string TaskhubName { get; set; }

        [DataMember]
        public Guid TaskhubGuid { get; set; }

        [DataMember]
        public DateTime CreationTimestamp { get; set; }

        [DataMember]
        public string StorageFormat { get; set; }

        [DataMember]
        public int PartitionCount { get; set; }
    }
}
