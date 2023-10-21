// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.EventHubs
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using Newtonsoft.Json;

    public struct Event
    {
        public int Destination { get; set; }
    
        public string Payload { get; set; }

        public static Event FromStream(Stream s)
        {
            var r = new BinaryReader(s);
            return new Event
            {
                Destination = r.ReadInt32(),
                Payload = r.ReadString(),
            };
        }

        public byte[] ToBytes()
        {
            var m = new MemoryStream();
            var r = new BinaryWriter(m);
            r.Write(this.Destination);
            r.Write(this.Payload);
            return m.ToArray();
        }
    }
}
