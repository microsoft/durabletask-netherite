// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
        // we prefix packets with a byte indicating the version, to facilitate format changes in the future
        static readonly byte version = 2;

        public static void Serialize(Event evt, Stream stream, byte[] taskHubGuid)
        {
            var writer = new BinaryWriter(stream, Encoding.UTF8);

            // first come the version and the taskhub
            writer.Write(Packet.version);
            writer.Write(taskHubGuid);
            writer.Flush();

            // then we write the binary serialization to the stream
            Serializer.SerializeEvent(evt, stream);
        }

        public static void Deserialize<TEvent>(Stream stream, out TEvent evt, byte[] taskHubGuid) where TEvent : Event
        {
            var reader = new BinaryReader(stream);
            var version = reader.ReadByte();
            var destinationTaskHubGuid = reader.ReadBytes(16);

            if (taskHubGuid != null && !GuidMatches(taskHubGuid, destinationTaskHubGuid))
            {
                evt = null;
                return;
            }

            if (version == Packet.version)
            {
                evt = (TEvent)Serializer.DeserializeEvent(stream);
            }
            else
            {
                throw new VersionNotFoundException($"Received packet with unsupported version {version} - likely a versioning issue");
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
