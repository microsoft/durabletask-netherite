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
    using Newtonsoft.Json;

    /// <summary>
    /// An orchestration that searches for hash collisions using a recursive divide-and-conquer algorithm.
    /// </summary>
    public static class FlatParallelHttpSearch
    {
        [FunctionName(nameof(FlatParallelHttpSearch))]
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

                async Task<List<long>> CallHttpAsync(int target, long start, long count)
                {
                    int retries = 10;

                    while (true)
                    {
                        try
                        {
                            DurableHttpRequest request = new DurableHttpRequest(
                                System.Net.Http.HttpMethod.Post,
                                new Uri($"https://functionssb1.azurewebsites.net/CollisionSearch/Portion/{target}/{start}/{count}"));
                            //new Uri($"http://localhost:7071/CollisionSearch/Portion/{target}/{start}/{count}"));

                            retries--;
                            DurableHttpResponse response = await context.CallHttpAsync(request);

                            if (response.StatusCode == System.Net.HttpStatusCode.OK)
                            {
                                return JsonConvert.DeserializeObject<List<long>>(response.Content);
                            }
                            else if (response.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                            {
                                logger.LogWarning("received throttle");
                                continue;
                            }
                            else
                            {
                                logger.LogError($"Received {response.StatusCode} response: {request.Content}");
                                if (retries > 0)
                                {
                                    continue;
                                }
                                else
                                {
                                    throw new Exception($"Received {response.StatusCode} response: {request.Content}");
                                }
                            }
                        }
                        catch(Exception e)
                        {
                            logger.LogError($"Caught exception {e}");
                            if (retries > 0)
                            {
                                continue;
                            }
                            else
                            {
                                throw new Exception($"Caught exception {e}");
                            }
                        }
                    }
                }

                tasks.Add(CallHttpAsync(input.Target, position, nextPortion));
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
