// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using System.Collections.Generic;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using System.Linq;
    using System.Collections.Concurrent;
    using System.Threading;

    /// <summary>
    /// A simple microbenchmark orchestration that calls three trivial activities in a sequence.
    /// </summary>
    public static class HelloCities
    {
        [FunctionName(nameof(HelloCities))]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "hellocities")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            int? numberOrchestrations = string.IsNullOrEmpty(requestBody) || requestBody == "null" ? null : (int?) JsonConvert.DeserializeObject<int>(requestBody);
            TimeSpan timeout = TimeSpan.FromSeconds(200);

            if (!numberOrchestrations.HasValue)
            {
                // we are running a single orchestration. 
                string orchestrationInstanceId = await client.StartNewAsync(nameof(HelloSequence));
                var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, timeout);
                return response;
            }
            else
            {
                // call several orchestrations in a loop and wait for all of them to complete
                var testname = Util.MakeTestName(req);
                try
                {

                    log.LogWarning($"Starting {testname} {numberOrchestrations.Value}...");

                    var stopwatch = new System.Diagnostics.Stopwatch();
                    stopwatch.Start();

                    var tasks = new List<Task<bool>>();

                    async Task<bool> RunOrchestration(int iteration)
                    {
                        var startTime = DateTime.UtcNow;
                        var orchestrationInstanceId = $"Orch{iteration}";

                        log.LogInformation($"{testname} starting {orchestrationInstanceId}");

                        await client.StartNewAsync(nameof(HelloSequence), orchestrationInstanceId);
                        await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, timeout);

                        if (DateTime.UtcNow < startTime + timeout)
                        {
                            log.LogInformation($"{testname} completed {orchestrationInstanceId}");
                            return true;
                        }
                        else
                        {
                            log.LogInformation($"{testname} timeout {orchestrationInstanceId}");
                            return false;
                        }
                    }

                    for (int i = 0; i < numberOrchestrations; i++)
                    {
                        tasks.Add(RunOrchestration(i));
                    }

                    await Task.WhenAll(tasks);

                    stopwatch.Stop();

                    int timeouts = tasks.Count(t => !t.Result);
                    double elapsedSeconds = elapsedSeconds = stopwatch.Elapsed.TotalSeconds;

                    log.LogWarning($"Completed {testname} with {timeouts} timeouts in {elapsedSeconds}s.");

                    object resultObject = timeouts > 0 ? (object)new { testname, timeouts } : new { testname, elapsedSeconds };

                    string resultString = $"{JsonConvert.SerializeObject(resultObject, Formatting.None)}\n";

                    return new OkObjectResult(resultString);
                }
                catch (Exception e)
                {
                    return new ObjectResult(
                        new
                        {
                            testname,
                            error = e.ToString(),
                        });
                }
            }
        }

        [FunctionName(nameof(CountCities))]
        public static async Task<IActionResult> CountCities(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "countcities")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            var queryCondition = new OrchestrationStatusQueryCondition()
            {
                InstanceIdPrefix = "Orch",
            };

            int completed = 0;
            int pending = 0;
            int running = 0;
            int other = 0;

            do
            {
                OrchestrationStatusQueryResult result = await client.ListInstancesAsync(queryCondition, CancellationToken.None);
                queryCondition.ContinuationToken = result.ContinuationToken;

                foreach (var status in result.DurableOrchestrationState)
                {
                    if (status.RuntimeStatus == OrchestrationRuntimeStatus.Pending)
                    {
                        pending++;
                    }
                    else if (status.RuntimeStatus == OrchestrationRuntimeStatus.Running)
                    {
                        running++;
                    }
                    else if (status.RuntimeStatus == OrchestrationRuntimeStatus.Completed)
                    {
                        completed++;
                    }
                    else
                    {
                        other++;
                    }
                }
            } while (queryCondition.ContinuationToken != null);

            return new OkObjectResult($"{pending+running+completed+other} orchestration instances ({pending} pending, {running} running, {completed} completed, {other} other)\n");
        }

        [FunctionName(nameof(PurgeCities))]
        public static async Task<IActionResult> PurgeCities(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "purgecities")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            var queryCondition = new OrchestrationStatusQueryCondition()
            {
                InstanceIdPrefix = "Orch",
            };

            PurgeHistoryResult result = await client.PurgeInstanceHistoryAsync(default,default,default);

            return new OkObjectResult($"purged {result.InstancesDeleted} orchestration instances.\n");
        }


        [FunctionName(nameof(HelloSequence))]
        public static async Task<List<string>> HelloSequence([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var result = new List<string>
            {
                await context.CallActivityAsync<string>(nameof(SayHello), "Tokyo"),
                await context.CallActivityAsync<string>(nameof(SayHello), "Seattle"),
                await context.CallActivityAsync<string>(nameof(SayHello), "London")
            };

            return result;
        }

        [FunctionName(nameof(SayHello))]
        public static Task<string> SayHello([ActivityTrigger] IDurableActivityContext context)
        {
            return Task.FromResult(context.GetInput<string>());
        }
    }
}
