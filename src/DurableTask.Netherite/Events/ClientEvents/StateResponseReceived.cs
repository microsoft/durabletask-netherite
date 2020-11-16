// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    class StateResponseReceived : ClientEvent
    {
        [DataMember]
        public OrchestrationState OrchestrationState { get; set; }
    }
}
