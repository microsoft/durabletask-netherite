//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

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
