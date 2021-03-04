// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.WordCount
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Security.Cryptography.X509Certificates;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using Microsoft.Identity.Client;

    public static class Summary
    {
        public static EntityId GetEntityId()
        {
            return new EntityId(nameof(Summary), "");
        }

        public enum Ops
        {
            Init,
            Report,
        }

        public class Report
        {
            public int entryCount;
            public List<(int, string)> topWords;
        }

        public class SummaryState
        {
            public int waitCount; // goes down to zero once we have reports from all reducers
            public int entryCount; // total number of words counted
            public List<(int, string)> topWords; // the top 20 words
            public DateTime startTime;
            public DateTime completionTime;
        }

        [FunctionName(nameof(Summary))]
        public static Task HandleOperation(
            [EntityTrigger] IDurableEntityContext context, ILogger log)
        {
            var state = context.GetState(() => new SummaryState());

            switch (Enum.Parse<Ops>(context.OperationName))
            {
                case Ops.Init:
                    if (!int.TryParse(context.GetInput<string>(), out var reducerCount))
                    {
                        log.LogError($"{context.EntityId}: Error Parsing count {context.GetInput<string>()} into integer");
                    }
                    else
                    {
                        state.waitCount = reducerCount;
                        state.topWords = new List<(int, string)>();
                        state.entryCount = 0;
                        state.startTime = DateTime.UtcNow;
                        log.LogWarning($"{context.EntityId}: initialized, reducer count <- {reducerCount}");
                    }
                    break;

                case Ops.Report:
                    var report = context.GetInput<Report>();
                    state.waitCount--;
                    state.entryCount += report.entryCount;
                    state.topWords.AddRange(report.topWords);
                    state.topWords.Sort((a, b) => b.Item1.CompareTo(a.Item1)); // sort in reverse order so most frequent words are first      
                    if (state.topWords.Count > 20)
                    {
                        state.topWords.RemoveRange(20, state.topWords.Count - 20);
                    }
                    log.LogWarning($"{context.EntityId}: received report ({state.waitCount} left)");
                    if (state.waitCount == 0)
                    {
                        state.completionTime = DateTime.UtcNow;
                    }
       
                    break;
            }
            return Task.CompletedTask;
        }
    }
}
