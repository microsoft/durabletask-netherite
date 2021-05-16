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
    /// A simple microbenchmark orchestration that searches for hash collisions.
    /// </summary>
    public static class CollisionSearch
    {
        public struct IntervalSearchParameters
        {
            // the hash code for which we want to find a collision
            public int Target { get; set; }
            // The start of the interval
            public long Start { get; set; }
            // The size of the interval
            public long Count { get; set; }
        }

        const int MaxIntervalSize = 10000000;

        [FunctionName(nameof(SearchOrchestration))]
        public static async Task<List<long>> SearchOrchestration([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger logger)
        {
            // get the input
            var input = context.GetInput<IntervalSearchParameters>();
            List<long> results;

            logger.LogInformation($"{context.InstanceId} Start searching interval [{input.Start},{input.Start + input.Count})");

            // We search the interval using a recursive divide-and-conquer.
            if (input.Count <= MaxIntervalSize)
            {
                // the interval is small enough to handle in a single activity
                results = await context.CallActivityAsync<List<long>>(nameof(SearchActivity), input);
            }
            else
            {
                // we break the interval into 10 portions and call them in parallel
                long portionSize = input.Count / 10;
                var subOrchestratorTasks = new Task<List<long>>[10];
                for (int i = 0; i < 10; i++)
                {
                    subOrchestratorTasks[i] = context.CallSubOrchestratorAsync<List<long>>(nameof(SearchOrchestration), new IntervalSearchParameters()
                    {
                        Target = input.Target,
                        Start = input.Start + i * portionSize,
                        Count = (i < 9) ? portionSize : (input.Count - (9 * portionSize)),
                    });
                }

                await Task.WhenAll(subOrchestratorTasks);

                // combine all the returned results into a single list
                results = subOrchestratorTasks.SelectMany(t => t.Result).ToList();
            }

            logger.LogInformation($"{context.InstanceId} Found {results.Count} collisions in interval [{input.Start},{input.Start + input.Count})");
            return results;
        }

        [FunctionName(nameof(SearchActivity))]
        public static Task<List<long>> SearchActivity([ActivityTrigger] IDurableActivityContext context)
        {
            var input = context.GetInput<IntervalSearchParameters>();

            var results = new List<long>();
            for (var i = input.Start; i < input.Start + input.Count; i++)
            {
                if (i.GetHashCode() == input.Target)
                    results.Add(i);
            }

            return Task.FromResult(results);
        }
    }
}
