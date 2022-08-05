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

    public class RempPrinter : IListener
    {
        readonly TextWriter writer;
        List<WorkitemGroup> workitemGroups;

        public RempPrinter(TextWriter writer)
        {
            this.writer = writer;
        }

        public void WorkerHeader(string workerId, IEnumerable<WorkitemGroup> groups)
        {
            var groupsstring = string.Join(" ", groups.Select((g, i) => $"[{i},{g.Name},{(g.DegreeOfParallelism.HasValue ? g.DegreeOfParallelism.Value.ToString() : '-')}]"));
            this.writer.WriteLine($"@ {workerId} {groupsstring}");
            this.workitemGroups = groups.ToList();
        }

        public void WorkItem(long timeStamp, string workItemId, int group, double latencyMs, IEnumerable<NamedPayload> consumedMessages, IEnumerable<NamedPayload> producedMessages, bool allowSpeculation, InstanceState? instanceState)
        {
            this.writer.Write($"{new DateTime(timeStamp, DateTimeKind.Utc):o} {workItemId} {this.workitemGroups[group].Name} {latencyMs:F2} {allowSpeculation}");
            this.writer.Write($" in: {string.Join(",", consumedMessages.Select(m => $"{m.Id}({m.NumBytes})"))}");
            this.writer.Write($" out: {string.Join(",", producedMessages.Select(m => $"{m.Id}({m.NumBytes})"))}");
            if (instanceState.HasValue)
            {
                this.writer.Write($" state: {instanceState.Value.InstanceId} {(instanceState.Value.Updated.HasValue ? (instanceState.Value.Updated.Value > 0 ? instanceState.Value.Updated.ToString() : "deleted") : "none")}");
            }
            this.writer.WriteLine();
        }
    }
}
