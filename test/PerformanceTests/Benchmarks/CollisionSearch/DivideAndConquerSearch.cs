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
    /// An orchestration that searches for hash collisions using a recursive divide-and-conquer algorithm.
    /// </summary>
    public static class DivideAndConquerSearch
    {     
        [FunctionName(nameof(DivideAndConquerSearch))]
        public static async Task<List<long>> Run([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger logger)
        {
            // get the input
            var input = context.GetInput<IntervalSearchParameters>();
            List<long> results;

            logger.LogInformation($"{context.InstanceId} Start searching interval [{input.Start},{input.Start + input.Count})");

            // We search the interval using a recursive divide-and-conquer.
            if (input.Count <= SearchActivity.MaxIntervalSize)
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
                    subOrchestratorTasks[i] = context.CallSubOrchestratorAsync<List<long>>(nameof(DivideAndConquerSearch), new IntervalSearchParameters()
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
    }
}
