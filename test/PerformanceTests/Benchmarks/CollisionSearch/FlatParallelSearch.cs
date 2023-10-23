// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.CollisionSearch
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging;
    using System.Collections.Generic;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using System.Linq;

    /// <summary>
    /// An orchestration that searches for hash collisions using the simple fan-out-fan-in pattern.
    /// </summary>
    public static class FlatParallelSearch
    {
        [FunctionName(nameof(FlatParallelSearch))]
        public static async Task<List<long>> Run([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger logger)
        {
            // get the input
            var input = context.GetInput<IntervalSearchParameters>();

            logger.LogInformation($"{context.InstanceId} Start searching interval [{input.Start},{input.Start + input.Count})");

            var tasks = new List<Task<List<long>>>();
            long position = input.Start;

            while (position < input.Start + input.Count)
            {
                long nextPortion = Math.Min(input.Start + input.Count - position, SearchActivity.MaxIntervalSize);

                tasks.Add(context.CallActivityAsync<List<long>>(
                        nameof(SearchActivity),
                        new IntervalSearchParameters()
                        {
                            Target = input.Target,
                            Start = position,
                            Count = nextPortion,
                        }));

                position += nextPortion;
            }

            await Task.WhenAll(tasks);

            // combine all the returned results into a single list
            var results = tasks.SelectMany(t => t.Result).ToList();

            logger.LogInformation($"{context.InstanceId} Found {results.Count} collisions in interval [{input.Start},{input.Start + input.Count})");
            return results;
        }
    }
}
