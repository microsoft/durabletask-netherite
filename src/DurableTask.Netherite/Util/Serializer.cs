//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.Netherite
{
    using System;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Text;

    static class Serializer
    {
        static readonly DataContractSerializer eventSerializer
            = new DataContractSerializer(typeof(Event));

        static readonly DataContractSerializer trackedObjectSerializer
            = new DataContractSerializer(typeof(TrackedObject));

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
    }
}
