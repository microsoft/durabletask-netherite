// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.IO;
    using DurableTask.Core;
    using System;

    [DataContract]
    class QueryResponseReceived : ClientEvent
    {
        // for efficiency, we use a binary representation with custom serialization and deserialization (see below)
        [IgnoreDataMember]
        public List<OrchestrationState> OrchestrationStates { get; set; }

        [DataMember]
        public DateTime Attempt { get; set; }

        [DataMember]
        public int? Final { get; set; }

        [DataMember]
        byte[] BinaryState { get; set; }

        [DataMember]
        public string ContinuationToken { get; set; }  // null indicates we have reached the end of all instances in this partition

        public void SerializeOrchestrationStates(MemoryStream memoryStream, bool includeInput)
        {
            var writer = new BinaryWriter(memoryStream);

            void writeNullableString(string s)
            {
                bool present = s != null;
                writer.Write((bool)present);
                if (present)
                {
                    writer.Write((string)s);
                }
            }
            void writeNullableDateTime(DateTime? d)
            {
                writer.Write((bool)d.HasValue);
                if (d.HasValue)
                {
                    writer.Write((long)d.Value.Ticks);
                }
            }

            writer.Write((int)this.OrchestrationStates.Count);
            foreach (var state in this.OrchestrationStates)
            {
                writeNullableString((string)state.Version);
                writeNullableString((string)state.Status);
                writeNullableString((string)state.Output);
                writeNullableString((string)state.Name);
                writeNullableString((string)(includeInput ? state.Input : null));
                writer.Write((string)state.OrchestrationInstance.InstanceId);
                writer.Write((string)state.OrchestrationInstance.ExecutionId);
                writer.Write((long)state.CompletedTime.Ticks);
                writer.Write((byte)state.OrchestrationStatus);
                writer.Write((long)state.LastUpdatedTime.Ticks);
                writer.Write((long)state.CreatedTime.Ticks);
                writeNullableDateTime(state.ScheduledStartTime);
            }
            writer.Flush();
            this.BinaryState = memoryStream.ToArray();
            memoryStream.Seek(0, SeekOrigin.Begin);
        }


        public void DeserializeOrchestrationStates()
        {
            this.OrchestrationStates = new List<OrchestrationState>();
            var memoryStream = new MemoryStream(this.BinaryState);
            using var reader = new BinaryReader(memoryStream);
            var numStates = reader.ReadInt32();
            for (int i = 0; i < numStates; i++)
            {
                var state = new OrchestrationState();
                state.OrchestrationInstance = new OrchestrationInstance();
                state.Version = reader.ReadBoolean() ? reader.ReadString() : null;
                state.Status = reader.ReadBoolean() ? reader.ReadString() : null;
                state.Output = reader.ReadBoolean() ? reader.ReadString() : null;
                state.Name = reader.ReadBoolean() ? reader.ReadString() : null;
                state.Input = reader.ReadBoolean() ? reader.ReadString() : null;
                state.OrchestrationInstance.InstanceId = reader.ReadString();
                state.OrchestrationInstance.ExecutionId = reader.ReadString();
                state.CompletedTime = new DateTime(reader.ReadInt64(), DateTimeKind.Utc);
                state.OrchestrationStatus = (OrchestrationStatus)reader.ReadByte();
                state.LastUpdatedTime = new DateTime(reader.ReadInt64(), DateTimeKind.Utc);
                state.CreatedTime = new DateTime(reader.ReadInt64(), DateTimeKind.Utc);
                state.ScheduledStartTime = reader.ReadBoolean() ? new DateTime(reader.ReadInt64(), DateTimeKind.Utc) : null;
                this.OrchestrationStates.Add(state);
            }
        }
    }
}
