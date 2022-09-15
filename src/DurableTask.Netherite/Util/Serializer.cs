// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Netherite.Faster;

    static class Serializer
    {
        static readonly DataContractSerializer eventSerializer
            = new DataContractSerializer(typeof(Event), Event.KnownTypes());

        static readonly DataContractSerializer trackedObjectSerializer
            = new DataContractSerializer(typeof(TrackedObject), TrackedObject.KnownTypes());

        static readonly DataContractSerializer singletonsSerializer
            = new DataContractSerializer(typeof(TrackedObject[]), TrackedObject.KnownTypes());

        static readonly DataContractSerializer partitionBatchSerializer
           = new DataContractSerializer(typeof(List<PartitionEvent>), Event.KnownTypes());

        static readonly DataContractSerializer clientBatchSerializer
           = new DataContractSerializer(typeof(List<ClientEvent>), Event.KnownTypes());

        static readonly DataContractSerializer loadMonitorBatchSerializer
           = new DataContractSerializer(typeof(List<LoadMonitorEvent>), Event.KnownTypes());

        static readonly DataContractSerializer checkpointInfoSerializer
            = new DataContractSerializer(typeof(CheckpointInfo));

        static readonly UnicodeEncoding uniEncoding = new UnicodeEncoding();

        public static byte[] SerializeEvent(Event e, byte? header = null)
        {
            var stream = new MemoryStream();
            if (header != null)
            {
                stream.WriteByte(header.Value);
            }
            eventSerializer.WriteObject(stream, e);
            return stream.ToArray();
        }

        public static void SerializeEvent(Event e, Stream s)
        {
            eventSerializer.WriteObject(s, e);
        }

        public static Event DeserializeEvent(ArraySegment<byte> bytes)
        {
            var stream = new MemoryStream(bytes.Array, bytes.Offset, bytes.Count);
            return (Event)eventSerializer.ReadObject(stream);
        }

        public static Event DeserializeEvent(byte[] bytes)
        {
            var stream = new MemoryStream(bytes);
            return (Event)eventSerializer.ReadObject(stream);
        }

        public static Event DeserializeEvent(Stream stream)
        {
            return (Event)eventSerializer.ReadObject(stream);
        }

        public static void SerializePartitionBatch(List<PartitionEvent> batch, Stream s)
        {
            partitionBatchSerializer.WriteObject(s, batch);
        }

        public static List<PartitionEvent> DeserializePartitionBatch(Stream stream)
        {
            return (List<PartitionEvent>) partitionBatchSerializer.ReadObject(stream);
        }

        public static void SerializeClientBatch(List<ClientEvent> batch, Stream s)
        {
            clientBatchSerializer.WriteObject(s, batch);
        }

        public static List<ClientEvent> DeserializeClientBatch(Stream stream)
        {
            return (List<ClientEvent>)clientBatchSerializer.ReadObject(stream);
        }

        public static void SerializeLoadMonitorBatch(List<LoadMonitorEvent> batch, Stream s)
        {
            loadMonitorBatchSerializer.WriteObject(s, batch);
        }

        public static List<LoadMonitorEvent> DeserializeLoadMonitorBatch(Stream stream)
        {
            return (List<LoadMonitorEvent>)loadMonitorBatchSerializer.ReadObject(stream);
        }

        public static byte[] SerializeTrackedObject(TrackedObject trackedObject)
        {
            var stream = new MemoryStream();
            trackedObjectSerializer.WriteObject(stream, trackedObject);
            return stream.ToArray();
        }

        public static TrackedObject DeserializeTrackedObject(byte[] bytes)
        {
            var stream = new MemoryStream(bytes);
            var result = (TrackedObject)trackedObjectSerializer.ReadObject(stream);
            return result;
        }

        public static byte[] SerializeSingletons(TrackedObject[] singletons)
        {
            var stream = new MemoryStream();
            singletonsSerializer.WriteObject(stream, singletons);
            return stream.ToArray();
        }

        public static TrackedObject[] DeserializeSingletons(Stream stream)
        {
            var result = (TrackedObject[])singletonsSerializer.ReadObject(stream);
            return result;
        }

        public static MemoryStream SerializeCheckpointInfo(CheckpointInfo checkpointInfo)
        {
            var stream = new MemoryStream();
            checkpointInfoSerializer.WriteObject(stream, checkpointInfo);
            return stream;
        }

        public static CheckpointInfo DeserializeCheckpointInfo(Stream stream)
        {
            var result = (CheckpointInfo)checkpointInfoSerializer.ReadObject(stream);
            return result;
        }
    }
}
