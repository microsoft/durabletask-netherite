// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Runtime.Serialization;

    [DataContract]
    class ClientEventFragment : ClientEvent, FragmentationAndReassembly.IEventFragment
    {
        [DataMember]
        public EventId OriginalEventId {  get; set; }

        [DataMember]
        public Guid? GroupId { get; set; } // we now use a group id for tracking fragments, to fix issue #231

        [DataMember]
        public byte[] Bytes { get; set; }

        [DataMember]
        public int Fragment { get; set; }

        [DataMember]
        public bool IsLast { get; set; }

        public override EventId EventId => EventId.MakeSubEventId(this.OriginalEventId, this.Fragment);
    }
}