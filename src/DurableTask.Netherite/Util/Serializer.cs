﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Netherite.Faster;

    static class Serializer
    {
        static readonly DataContractSerializer eventSerializer
            = new DataContractSerializer(typeof(Event));

        static readonly DataContractSerializer trackedObjectSerializer
            = new DataContractSerializer(typeof(TrackedObject));

        static readonly DataContractSerializer singletonsSerializer
            = new DataContractSerializer(typeof(TrackedObject[]));

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

        public static void SerializeTrackedObject(TrackedObject trackedObject)
        {
            if (trackedObject.SerializationCache == null)
            {
                var stream = new MemoryStream();
                trackedObjectSerializer.WriteObject(stream, trackedObject);
                trackedObject.SerializationCache = stream.ToArray();
            }
        }

        public static TrackedObject DeserializeTrackedObject(byte[] bytes)
        {
            var stream = new MemoryStream(bytes);
            var result = (TrackedObject)trackedObjectSerializer.ReadObject(stream);
            result.SerializationCache = bytes;
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
