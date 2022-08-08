// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Orchestrations.Counter
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Newtonsoft.Json;

    public static class HttpTriggers
    {
        [FunctionName(nameof(WaitFor))]
        public static async Task<IActionResult> WaitFor(
            [HttpTrigger(AuthorizationLevel.Function, methods: "get", Route = "counter/{key}/waitfor/{count}")] HttpRequest req,
            string key, 
            int count,
            [DurableClient] IDurableClient client)
        {
            var entityId = new EntityId("Counter", key);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            // poll the entity until the expected count is reached
            while (stopwatch.Elapsed < TimeSpan.FromMinutes(5))
            {
                var response = await client.ReadEntityStateAsync<Counter>(entityId);

                if (response.EntityExists
                    && response.EntityState.CurrentValue >= count)
                {
                    return new OkObjectResult($"{JsonConvert.SerializeObject(response.EntityState)}\n");
                }

                await Task.Delay(TimeSpan.FromSeconds(2));
            }

            return new OkObjectResult("timed out.\n");
        }

        [FunctionName(nameof(Add))]
        public static async Task<IActionResult> Add(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "counter/{key}/add")] HttpRequest req,
            string key,
            [DurableClient] IDurableClient client)
        {
            try
            {
                string input = await new StreamReader(req.Body).ReadToEndAsync();
                int amount = int.Parse(input);
                var entityId = new EntityId("Counter", key);
                await client.SignalEntityAsync(entityId, "add", amount);
                return new OkObjectResult($"add({amount}) was sent to {entityId}.\n");
            }
            catch (Exception e)
            {
                return new OkObjectResult(e.ToString());
            }
        }

        [FunctionName(nameof(GetCounter))]
        public static async Task<IActionResult> GetCounter(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "counter/{key}")] HttpRequest req,
            string key,
            [DurableClient] IDurableClient client)
        {
            try
            {
                var entityId = new EntityId("Counter", key);
                var response = await client.ReadEntityStateAsync<Counter>(entityId);

                if (!response.EntityExists)
                {
                    return new NotFoundObjectResult($"no such entity: {entityId}");
                }
                else
                {
                    return new OkObjectResult(response.EntityState);
                }
            }
            catch (Exception e)
            {
                return new OkObjectResult(e.ToString());
            }
        }

        [FunctionName(nameof(Crash))]
        public static async Task<IActionResult> Crash(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "counter/{key}/crash")] HttpRequest req,
           string key,
           [DurableClient] IDurableClient client)
        {
            try
            {
                var entityId = new EntityId("Counter", key);
                await client.SignalEntityAsync(entityId, "crash", DateTime.UtcNow);
                return new OkObjectResult($"crash was sent to {entityId}.\n");
            }
            catch (Exception e)
            {
                return new OkObjectResult(e.ToString());
            }
        }

        [FunctionName(nameof(CountSignals))]
        public static async Task<IActionResult> CountSignals(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", nameof(CountSignals))] HttpRequest req,
            [DurableClient] IDurableClient client)
        {
            try
            {
                int numberSignals = int.Parse(await new StreamReader(req.Body).ReadToEndAsync());
                var entityId = new EntityId("Counter", Guid.NewGuid().ToString("N"));

                DateTime startTime = DateTime.UtcNow;

                // send the specified number of signals to the entity
                await SendIncrementSignals(client, numberSignals, 50, (i) => entityId);

                // poll the entity until the expected count is reached
                while ((DateTime.UtcNow - startTime) < TimeSpan.FromMinutes(5))
                {
                    var response = await client.ReadEntityStateAsync<Counter>(entityId);

                    if (response.EntityExists
                        && response.EntityState.CurrentValue == numberSignals)
                    {
                        return new OkObjectResult($"received {numberSignals} signals in {(response.EntityState.LastModified - startTime).TotalSeconds:F1}s.\n");
                    }

                    await Task.Delay(TimeSpan.FromSeconds(2));
                }

                return new OkObjectResult($"timed out after {(DateTime.UtcNow - startTime)}.\n");
            }
            catch (Exception e)
            {
                return new OkObjectResult(e.ToString());
            }
        }

        [FunctionName(nameof(CountParallelSignals))]
        public static async Task<IActionResult> CountParallelSignals(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = nameof(CountParallelSignals))] HttpRequest req,
            [DurableClient] IDurableClient client)
        {
            try
            {
                // input is of the form "nnn,mmm"
                // nnn - number of signals to send
                // mmm - number of entities to distribute the signals over

                string input = await new StreamReader(req.Body).ReadToEndAsync();
                int commaPosition = input.IndexOf(',');
                int numberSignals = int.Parse(input.Substring(0, commaPosition));
                int numberEntities = int.Parse(input.Substring(commaPosition + 1));
                var entityPrefix = Guid.NewGuid().ToString("N");
                EntityId MakeEntityId(int i) => new EntityId("Counter", $"{entityPrefix}-!{i:D6}");
                DateTime startTime = DateTime.UtcNow;

                if (numberSignals % numberEntities != 0)
                {
                    throw new ArgumentException("numberSignals must be a multiple of numberEntities");
                }

                // send the specified number of signals to the entity
                await SendIncrementSignals(client, numberSignals, 50, (i) => MakeEntityId(i % numberEntities));

                // poll the entities until the expected count is reached
                async Task<double?> WaitForCount(int i)
                {
                    var random = new Random();

                    while ((DateTime.UtcNow - startTime) < TimeSpan.FromMinutes(5))
                    {
                        var response = await client.ReadEntityStateAsync<Counter>(MakeEntityId(i));

                        if (response.EntityExists
                            && response.EntityState.CurrentValue == numberSignals / numberEntities)
                        {
                            return (response.EntityState.LastModified - startTime).TotalSeconds;
                        }

                        await Task.Delay(TimeSpan.FromSeconds(2 + random.NextDouble()));
                    }

                    return null;
                };

                var waitTasks = Enumerable.Range(0, numberEntities).Select(i => WaitForCount(i)).ToList();

                await Task.WhenAll(waitTasks);

                var results = waitTasks.Select(t => t.Result);

                if (results.Any(result => result == null))
                {
                    return new OkObjectResult($"timed out after {(DateTime.UtcNow - startTime)}.\n");
                }

                return new OkObjectResult($"received {numberSignals} signals on {numberEntities} entities in {results.Max():F1}s.\n");
            }
            catch (Exception e)
            {
                return new OkObjectResult(e.ToString());
            }
        }

        static async Task SendIncrementSignals(IDurableClient client, int numberSignals, int maxConcurrency, Func<int, EntityId> entityIdFactory)
        {
            // send the specified number of signals to the entity
            // for better throughput we do this in parallel

            using var semaphore = new SemaphoreSlim(maxConcurrency);

            for (int i = 0; i < numberSignals; i++)
            {
                var entityId = entityIdFactory(i);
                await semaphore.WaitAsync();
                var task = Task.Run(async () =>
                {
                    try
                    {
                        await client.SignalEntityAsync(entityId, "add", 1);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                });
            }

            // wait for tasks to complete
            for (int i = 0; i < maxConcurrency; i++)
            {
                await semaphore.WaitAsync();
            }
        }
    }
}
