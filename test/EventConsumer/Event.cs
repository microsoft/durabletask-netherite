// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace EventConsumer
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Newtonsoft.Json;

    [JsonObject(MemberSerialization.OptOut)]
    public struct Event
    {
        public int Partition { get; set; }

        public long SeqNo { get; set; }

        public byte[] Payload { get; set; }

        public override string ToString()
        {
            return $"Event {this.Partition}.{this.SeqNo} size={this.Payload.Length}";
        }
    }
}
