// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using Dynamitey;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.IO;
    using System.Reflection.Emit;
    using System.Text;

    /// <summary>
    /// Packets are the unit of transmission for external events, among clients and partitions.
    /// </summary>
    static class Packet
    {
        // we prefix packets with a byte indicating the format
        // we use a flag (for json) or a version (for binary) to facilitate changing formats in the future
        static readonly byte jsonVersion = 0;
        static readonly byte binaryVersion = 1;

        static readonly JsonSerializerSettings serializerSettings 
            = new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.Auto };

        public static void Serialize(Event evt, Stream stream, bool useJson, byte[] taskHubGuid)
        {
            var writer = new BinaryWriter(stream, Encoding.UTF8);

            if (useJson)
            {
                // serialize the json
                string jsonContent = JsonConvert.SerializeObject(evt, typeof(Event), Packet.serializerSettings);

                // first entry is the version and the taskhub
                writer.Write(Packet.jsonVersion);
                writer.Write(taskHubGuid);

                // then we write the json string
                writer.Write(jsonContent);
            }
            else
            {
                // first entry is the version and the taskhub
                writer.Write(Packet.binaryVersion);
                writer.Write(taskHubGuid);

                writer.Flush();

                // then we write the binary serialization to the stream
                Serializer.SerializeEvent(evt, stream);
            }
        }

        public static void Deserialize<TEvent>(Stream stream, out TEvent evt, byte[] taskHubGuid) where TEvent : Event
        {
            var reader = new BinaryReader(stream);
            var format = reader.ReadByte();
            var destinationTaskHubGuid = reader.ReadBytes(16);

            if (taskHubGuid != null && !GuidMatches(taskHubGuid, destinationTaskHubGuid))
            {
                evt = null;
                return;
            }

            if (format == Packet.jsonVersion)
            {
                string jsonContent = reader.ReadString();
                evt = (TEvent)JsonConvert.DeserializeObject(jsonContent, Packet.serializerSettings);
            }
            else if (format == Packet.binaryVersion)
            {
                evt = (TEvent)Serializer.DeserializeEvent(stream);
            }
            else
            {
                throw new VersionNotFoundException($"Received packet with unhandled format indicator {format} - likely a versioning issue");
            }
        }

        public static void Deserialize<TEvent>(ArraySegment<byte> arraySegment, out TEvent evt, byte[] taskHubGuid) where TEvent : Event
        {
            using (var stream = new MemoryStream(arraySegment.Array, arraySegment.Offset, arraySegment.Count, false))
            {
                Packet.Deserialize(stream, out evt, taskHubGuid);
            }
        }

        public static bool GuidMatches(byte[] expected, byte[] actual)
        {
            for (int i = 0; i < 16; i++)
            {
                if (expected[i] != actual[i])
                {
                    return false;
                }
            }

            return true;
        }
    }
}
