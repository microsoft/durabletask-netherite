// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
