// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Runtime.Serialization;
    using System.Text;

    [DataContract]
    class PurgeResponseReceived : ClientEvent
    {
        [DataMember]
        public int NumberInstancesPurged { get; set; }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(" count=");
            s.Append(this.NumberInstancesPurged);
        }
    }
}
