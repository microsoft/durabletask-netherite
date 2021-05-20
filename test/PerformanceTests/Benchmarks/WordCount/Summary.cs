// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.WordCount
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

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
            public List<KeyValuePair<string, int>> topWords;
        }

        public class SummaryState
        {
            public int waitCount; // goes down to zero once we have reports from all reducers
            public int entryCount; // total number of words counted
            public List<(int, string)> topWords; // the top 20 words
            public DateTime startTime;
            public DateTime completionTime;
            public double executionTimeInSeconds;
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

                    var wordCount = new Dictionary<string,int>(report.topWords);
                    foreach(var (count, word) in state.topWords)
                    {
                        wordCount.TryGetValue(word, out int currentCount);
                        wordCount[word] = currentCount + count;
                    }

                    state.topWords = wordCount.OrderByDescending(kvp => kvp.Value).Take(20).Select(kvp => (kvp.Value, kvp.Key)).ToList();

                    log.LogWarning($"{context.EntityId}: received report ({state.waitCount} left)");
                    if (state.waitCount == 0)
                    {
                        state.completionTime = DateTime.UtcNow;
                        state.executionTimeInSeconds = (state.completionTime - state.startTime).TotalSeconds;
                    }
       
                    break;
            }
            return Task.CompletedTask;
        }
    }
}
