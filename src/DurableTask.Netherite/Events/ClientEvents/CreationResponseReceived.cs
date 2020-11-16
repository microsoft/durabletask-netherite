// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    class CreationResponseReceived : ClientEvent
    {
        [DataMember]
        public bool Succeeded { get; set; }

        [DataMember]
        public OrchestrationStatus? ExistingInstanceOrchestrationStatus { get; set; }
    }
}
