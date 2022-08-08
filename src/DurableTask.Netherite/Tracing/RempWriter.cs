// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Remp
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using static RempFormat;

    public class RempWriter : BinaryWriter, IListener
    {
        public RempWriter(Stream stream)
            : base(stream)
        {
        }

        void Assert(bool condition, string name)
        {
            if (!condition)
            {
                throw new ArgumentException($"invalid field or parameter", name);
            }
        }

        public void WorkerHeader(string workerId, IEnumerable<WorkitemGroup> groups)
        {
            this.Write((long) RempFormat.CurrentVersion);
            this.Assert(!string.IsNullOrEmpty(workerId), nameof(workerId));
            this.Write(workerId);
            foreach (var group in groups)
            {
                this.Write(group);
            }
            this.Write(string.Empty); // terminates the group list
        }

        public void WorkItem(long timeStamp, string workItemId, int group, double latencyMs, IEnumerable<NamedPayload> consumedMessages, IEnumerable<NamedPayload> producedMessages, bool allowSpeculation, InstanceState? instanceState)
        {
            this.Assert(timeStamp > 100, nameof(timeStamp));
            this.Write(timeStamp);
            this.Assert(!string.IsNullOrEmpty(workItemId), nameof(workItemId));
            this.Write(workItemId);
            this.Write(group);
            this.Write(latencyMs);
            foreach (var message in consumedMessages)
            {
                this.Write(message);
            }
            this.Write(string.Empty); // terminates the message list
            foreach (var message in producedMessages)
            {
                this.Write(message);
            }
            this.Write(string.Empty); // terminates the message list
            this.Write(allowSpeculation);
            this.Write(instanceState.HasValue);
            if (instanceState.HasValue)
            {
                this.Write(instanceState.Value);
            }
        }

        public void Write(WorkitemGroup group)
        {
            this.Assert(!string.IsNullOrEmpty(group.Name), nameof(WorkitemGroup.Name));
            this.Write(group.Name);
            this.Write(group.DegreeOfParallelism.HasValue);
            if (group.DegreeOfParallelism.HasValue)
            {
                this.Assert(group.DegreeOfParallelism.Value > 0, nameof(WorkitemGroup.DegreeOfParallelism));
                this.Write(group.DegreeOfParallelism.Value);
            }
        }

        public void Write(NamedPayload namedPayload)
        {
            this.Assert(!string.IsNullOrEmpty(namedPayload.Id), nameof(NamedPayload.Id));
            this.Write(namedPayload.Id);
            this.Assert(namedPayload.NumBytes > 0, nameof(NamedPayload.NumBytes));
            this.Write(namedPayload.NumBytes);
        }

        public void Write(InstanceState instanceState)
        {
            this.Assert(!string.IsNullOrEmpty(instanceState.InstanceId), nameof(InstanceState.InstanceId));
            this.Write(instanceState.InstanceId);
            this.Write(instanceState.Updated.HasValue);
            if (instanceState.Updated.HasValue)
            {
                this.Write(instanceState.Updated.Value);
            }
            this.Write(instanceState.Delta.HasValue);
            if (instanceState.Delta.HasValue)
            {
                this.Write(instanceState.Delta.Value);
            }
        }
    }
}
