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

    /// <summary>
    /// A microbenchmark using durable entities for bank accounts, and an orchestration with a critical section for transferring
    /// currency between two accounts.
    /// </summary>
    public static class Bank
    {
        public interface IAccount
        {
            Task Add(int amount);
            Task Reset();
            Task<int> Get();
            void Delete();
        }

        [JsonObject(MemberSerialization.OptIn)]
        public class Account : IAccount
        {
            [JsonProperty("value")]
            public int Value { get; set; }

            public Task Add(int amount)
            {
                this.Value += amount;
                return Task.CompletedTask;
            }

            public Task Reset()
            {
                this.Value = 0;
                return Task.CompletedTask;
            }

            public Task<int> Get()
            {
                return Task.FromResult(this.Value);
            }

            public void Delete()
            {
                Entity.Current.DeleteState();
            }


        }

        [FunctionName(nameof(Account))]
        public static Task Dispatch([EntityTrigger] IDurableEntityContext context) => context.DispatchAsync<Account>();

        [FunctionName(nameof(Bank))]
        public static async Task<IActionResult> Run(
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


        [FunctionName(nameof(BankTransaction))]
        public static async Task<bool> BankTransaction([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var iterationLength = context.GetInput<Tuple<int, int>>();
            var targetAccountPair = iterationLength.Item1;
            var length = iterationLength.Item2;

            var sourceAccountId = $"src{targetAccountPair}-!{(targetAccountPair + 1) % 32:D2}";
            var sourceEntity = new EntityId(nameof(Account), sourceAccountId);

            var destinationAccountId = $"dst{targetAccountPair}-!{(targetAccountPair + 2) % 32:D2}";
            var destinationEntity = new EntityId(nameof(Account), destinationAccountId);

            // Add an amount to the first account
            var transferAmount = 1000;

            IAccount sourceProxy =
                context.CreateEntityProxy<IAccount>(sourceEntity);
            IAccount destinationProxy =
                context.CreateEntityProxy<IAccount>(destinationEntity);

            // we want the balance check to always succeeed to reduce noise
            bool forceSuccess = true;

            // Create a critical section to avoid race conditions.
            // No operations can be performed on either the source or
            // destination accounts until the locks are released.
            using (await context.LockAsync(sourceEntity, destinationEntity))
            {
                int sourceBalance = await sourceProxy.Get();

                if (sourceBalance > transferAmount || forceSuccess)
                {
                    await sourceProxy.Add(-transferAmount);
                    await destinationProxy.Add(transferAmount);

                    return true;
                }
                else
                {
                    return false;
                }    
            }
        }
    }
}
