// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
