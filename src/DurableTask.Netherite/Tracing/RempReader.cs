// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tracing
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using static DurableTask.Netherite.Tracing.RempFormat;

    public class RempReader : BinaryReader
    {
        public RempReader(Stream stream)
            : base(stream)
        {
        }

        void Assert(bool condition, string message)
        {
            if (!condition)
            {
                throw new FormatException($"invalid file format: {message}");
            }
        }

        public void Read(RempFormat.IListener listener)
        {
            long versionOrTimestamp = this.ReadInt64();
            if (versionOrTimestamp < 100)
            {
                // we are reading a worker header
                long version = versionOrTimestamp;
                this.Assert(version == RempFormat.CurrentVersion, $"format version mismatch, expected={RempFormat.CurrentVersion}, actual={version}");
                string workerId = this.ReadString();
                IEnumerable<WorkitemGroup> groups = this.ReadList(this.ReadWorkitemGroup).ToList();
                listener.WorkerHeader(workerId, groups);
            }
            else
            {
                // we are reading a work item
                long timeStamp = versionOrTimestamp;
                string workItemId = this.ReadString();
                int group = this.ReadInt32();
                double latencyMs = this.ReadDouble();
                IEnumerable<NamedPayload> consumedMessages = this.ReadList(this.ReadNamedPayload).ToList();
                IEnumerable<NamedPayload> producedMessages = this.ReadList(this.ReadNamedPayload).ToList();
                bool allowSpeculation = this.ReadBoolean();
                InstanceState? instanceState = this.ReadBoolean() ? this.ReadInstanceState() : null;
                listener.WorkItem(timeStamp, workItemId, group, latencyMs, consumedMessages, producedMessages, allowSpeculation, instanceState);
            }
        }

        IEnumerable<T> ReadList<T>(Func<string, T> readNext)
        {
            string lookahead = this.ReadString();
            if (lookahead.Length == 0)
            {
                // this is the termination marker
                yield break;
            }
            else
            {
                yield return readNext(lookahead);
            }
        }

        public WorkitemGroup ReadWorkitemGroup(string lookahead)
        {
            return new WorkitemGroup()
            {
                Name = lookahead,
                DegreeOfParallelism = this.ReadBoolean() ? this.ReadInt32() : null,
            };
        }

        public NamedPayload ReadNamedPayload(string lookahead)
        {
            return new NamedPayload()
            {
                Id = lookahead,
                NumBytes = this.ReadInt64(),
            };
        }

        public InstanceState ReadInstanceState()
        {
            return new InstanceState()
            {
                InstanceId = this.ReadString(),
                Updated = this.ReadBoolean() ? this.ReadInt64() : null,
            };
        }
    }
}
