// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using Azure.Storage.Blobs.Models;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
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
        // we prefix packets with a byte indicating the packet type and whether it contains a guid
        // (we can also use this for version changes over time)
        static readonly byte eventWithGuid = 2;
        static readonly byte batchWithGuid = 3;
        static readonly byte eventWithoutGuid = 4;

        public static void Serialize(Event evt, Stream stream, byte[] taskHubGuid)
        {
            var writer = new BinaryWriter(stream, Encoding.UTF8);

            // first write the packet type and the taskhub
            writer.Write(Packet.eventWithGuid);
            writer.Write(taskHubGuid);
            writer.Flush();

            // then we write the binary serialization to the stream
            Serializer.SerializeEvent(evt, stream);
        }

        public static void Serialize(Event evt, Stream stream)
        {
            var writer = new BinaryWriter(stream, Encoding.UTF8);

            // first write the packet type and the taskhub
            writer.Write(Packet.eventWithoutGuid);
            writer.Flush();

            // then we write the binary serialization to the stream
            Serializer.SerializeEvent(evt, stream);
        }

        public static void Serialize(string blobAddress, List<int> packetOffsets, Stream stream, byte[] taskHubGuid)
        {
            var writer = new BinaryWriter(stream, Encoding.UTF8);

            // first write the packet type and the taskhub
            writer.Write(Packet.batchWithGuid);
            writer.Write(taskHubGuid);

            // then write the blob Address and the positions
            writer.Write(blobAddress);
            writer.Write(packetOffsets.Count);
            foreach(var p in packetOffsets)
            {
                writer.Write(p);
            }
            writer.Flush();
        }


        public class BlobReference
        {
            public string BlobName;
            public List<int> PacketOffsets;
        }


        public static void Deserialize<TEvent>(Stream stream, out TEvent evt, out BlobReference blobReference, byte[] taskHubGuid) where TEvent : Event
        {
            var reader = new BinaryReader(stream);
            var packetType = reader.ReadByte();
            evt = null;
            blobReference = null;

            if (packetType == Packet.eventWithGuid)
            {
                byte[] destinationTaskHubId = reader.ReadBytes(16);
                if (taskHubGuid != null && !GuidMatches(taskHubGuid, destinationTaskHubId))
                {
                    return;
                }
                evt = (TEvent)Serializer.DeserializeEvent(stream);
            }
            else if (packetType == Packet.batchWithGuid)
            {
                byte[] destinationTaskHubId = reader.ReadBytes(16);
                if (taskHubGuid != null && !GuidMatches(taskHubGuid, destinationTaskHubId))
                {
                    return;
                }
                string blobName = reader.ReadString();
                int numEvents = reader.ReadInt32();
                List<int> packetOffsets = new List<int>(numEvents);
                for (int i = 0; i < numEvents; i++)
                {
                    packetOffsets.Add(reader.ReadInt32());
                }
                blobReference = new BlobReference()
                {
                    BlobName = blobName,
                    PacketOffsets = packetOffsets
                };
            }
            else if (packetType == Packet.eventWithoutGuid)
            {
                evt = (TEvent)Serializer.DeserializeEvent(stream);
            }
            else
            {
                throw new VersionNotFoundException($"Received packet with unsupported packet type {packetType} - likely a versioning issue");
            }           
        }

        public static void Deserialize<TEvent>(ArraySegment<byte> arraySegment, out TEvent evt, out BlobReference blobReference, byte[] taskHubGuid) where TEvent : Event
        {
            using (var stream = new MemoryStream(arraySegment.Array, arraySegment.Offset, arraySegment.Count, false))
            {
                Packet.Deserialize(stream, out evt, out blobReference, taskHubGuid);
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
