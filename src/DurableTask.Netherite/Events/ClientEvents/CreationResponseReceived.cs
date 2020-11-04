// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
