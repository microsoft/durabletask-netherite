// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;

    [DataContract]
    class WorkerRequestReceived : WorkerEvent
    {
        [DataMember]
        public string OriginWorkItemId;

        [DataMember]
        public TaskMessage Message;

        public override EventId EventId => EventId.MakeWorkerRequestEventId(this.OriginWorkItemId, this.Message.SequenceNumber);
    }
}
