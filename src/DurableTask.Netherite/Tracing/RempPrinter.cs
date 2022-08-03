// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tracing
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Microsoft.Azure.Documents.SystemFunctions;
    using static DurableTask.Netherite.Tracing.RempFormat;

    public class RempPrinter : StreamWriter, IListener
    {
        public RempPrinter(Stream stream)
            : base(stream)
        {
        }

        public void WorkerHeader(string workerId, IEnumerable<WorkitemGroup> groups)
        {
            var groupsstring = string.Join(",", groups.Select((g, i) => $"{i}={g.Name},{(g.DegreeOfParallelism.HasValue ? g.DegreeOfParallelism.Value.ToString() : '-')}"));
            this.WriteLine($"WorkerId={workerId} groups: {groupsstring}");
        }

        public void WorkItem(long timeStamp, string workItemId, int group, double latencyMs, IEnumerable<NamedPayload> consumedMessages, IEnumerable<NamedPayload> producedMessages, bool allowSpeculation, InstanceState? instanceState)
        {
            this.WriteLine($"{new DateTime(timeStamp, DateTimeKind.Utc):o} workItemId={workItemId} group={group} latencyMs={latencyMs:F2} allowSpeculation={allowSpeculation}");
            this.WriteLine($"   consumed messages: {string.Join(" ", consumedMessages.Select(m => $"{m.Id}({m.NumBytes})"))}");
            this.WriteLine($"   produced messages: {string.Join(" ", producedMessages.Select(m => $"{m.Id}({m.NumBytes})"))}");
            if (instanceState.HasValue)
            {
                this.WriteLine($"   state: instanceId={instanceState.Value.InstanceId} update={(instanceState.Value.Updated.HasValue ? (instanceState.Value.Updated.Value > 0 ? instanceState.Value.ToString() : "deleted") : "none")}");
            }
        }
    }
}
