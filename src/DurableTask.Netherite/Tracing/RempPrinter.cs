// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Remp
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Microsoft.Azure.Documents.SystemFunctions;
    using static RempFormat;

    /// <summary>
    /// Prints a REMP trace in a format suitable for visual inspection by humans.
    /// Not intended to be parsed, since we already have a binary format.
    /// </summary>
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
                string StateSize() => instanceState.Value.Updated.HasValue ? (instanceState.Value.Updated.Value > 0 ? instanceState.Value.Updated.ToString() : "deleted") : "none";
                string DeltaSize() => instanceState.Value.Delta.HasValue ? $" (+{instanceState.Value.Delta.Value})" : "";
                this.writer.Write($" state: {instanceState.Value.InstanceId} {StateSize()}{DeltaSize()}");
            }
            this.writer.WriteLine();
        }
    }
}
