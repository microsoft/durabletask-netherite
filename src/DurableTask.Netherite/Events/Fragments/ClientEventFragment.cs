// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Runtime.Serialization;

    [DataContract]
    class ClientEventFragment : ClientEvent, FragmentationAndReassembly.IEventFragment
    {
        [DataMember]
        public EventId OriginalEventId {  get; set; }

        [DataMember]
        public byte[] Bytes { get; set; }

        [DataMember]
        public int Fragment { get; set; }

        [DataMember]
        public bool IsLast { get; set; }

        public override EventId EventId => EventId.MakeSubEventId(this.OriginalEventId, this.Fragment);
    }
}