// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;

    [DataContract]
    class DeletionResponseReceived : ClientEvent
    {
        [DataMember]
        public int NumberInstancesDeleted { get; set; }
    }
}
