// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;
    using DurableTask.Netherite.Faster;
    using Dynamitey.DynamicObjects;

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

        static readonly DataContractSerializer taskMessageSerializer
          = new DataContractSerializer(typeof(Core.TaskMessage));

        static readonly DataContractSerializer historyEventSerializer
          = new DataContractSerializer(typeof(Core.History.HistoryEvent));


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

        public static long GetMessageSize(Core.TaskMessage taskMessage)
        {
            var stream = new MemoryStream();
            taskMessageSerializer.WriteObject(stream, taskMessage);
            return stream.Position;
        }

        public static long GetStateSize(OrchestrationRuntimeState newRuntimeState, OrchestrationState newOrchestrationState)
        {
            var stream = new MemoryStream();
            var writer = new BinaryWriter(stream);
            writer.Write(newRuntimeState.Events.Count);
            foreach(var evt in newRuntimeState.Events)
            {
                historyEventSerializer.WriteObject(stream, evt);
            }
            if (newOrchestrationState.Status != null)
            {
                writer.Write(false);
            }
            else
            {
                writer.Write(true);
                writer.Write(newOrchestrationState.Status != null);
                if (newOrchestrationState.Status != null)
                {
                    writer.Write(newOrchestrationState.Status);
                }
            }
            return stream.Position;
        }
    }
}
