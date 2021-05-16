// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Bank
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

    /// <summary>
    /// A microbenchmark using durable entities for bank accounts, and an orchestration with a critical section for transferring
    /// currency between two accounts.
    /// </summary>
    public static class HttpTriggers
    {
        [FunctionName(nameof(Bank))]
        public static async Task<IActionResult> Bank(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "bank")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            // If the request body endswith p, then we want to run the bank transactions in parallel.
            int numberOrchestrations;
            bool isParallel = false;
            int pos;
            if (requestBody.Contains("p"))
            {
                pos = requestBody.IndexOf('p');
                numberOrchestrations = int.Parse(requestBody.Substring(0, pos));
                isParallel = true;
            }
            else if (requestBody.Contains("s"))
            {
                pos = requestBody.IndexOf('s');
                numberOrchestrations = int.Parse(requestBody.Substring(0, pos));
            }
            else
            {
                return new ObjectResult(
                    new
                    {
                        error = "Unable to parse input",
                    });
            }
            var lengthTransaction = int.Parse(requestBody.Substring(pos + 1));
            var testname = Util.MakeTestName(req);
            bool waitForCompletion = true;

            try
            {
                log.LogWarning($"Starting {testname} ...");

                var stopwatch = new System.Diagnostics.Stopwatch();
                stopwatch.Start();

                var responseTimes = new List<long>();

                async Task<long> ExecuteTransaction(int iteration)
                {
                    var parallelString = isParallel ? "parallel" : "sequential";
                    var orchestrationInstanceId = $"Bank{numberOrchestrations}{parallelString}{lengthTransaction}-orchestration-{iteration}-!00";
                    var startTime = stopwatch.ElapsedMilliseconds;
                    var input = new Tuple<int, int>(iteration, lengthTransaction);
                    await client.StartNewAsync(nameof(BankTransaction), orchestrationInstanceId, input);
                    log.LogInformation($"{testname} started {orchestrationInstanceId}...");
                    if (waitForCompletion)
                    {
                        await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromMinutes(5));
                        log.LogInformation($"{testname} completed {orchestrationInstanceId}...");
                    }
                    return await Task<long>.FromResult(stopwatch.ElapsedMilliseconds - startTime);
                }

                if (isParallel)
                {
                    var tasks = new List<Task<long>>();
                    for (int i = 0; i < numberOrchestrations; i++)
                    {
                        tasks.Add(ExecuteTransaction(i));
                    }
                    await Task.WhenAll(tasks);
                    foreach (var task in tasks)
                    {
                        responseTimes.Add(task.Result);
                    }
                }
                else
                {
                    for (int i = 0; i < numberOrchestrations; i++)
                    {
                        var result = await ExecuteTransaction(i);
                        responseTimes.Add(result);
                    }
                }

                stopwatch.Stop();

                var averageResponseTime = responseTimes.Average();

                log.LogWarning($"Completed {testname}. Average Response Time (ms): {averageResponseTime}");

                object resultObject = new
                {
                    testname,
                    elapsedSeconds = stopwatch.Elapsed.TotalSeconds,
                    averageResponseTimeMs = averageResponseTime,
                };

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
}
