// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Orchestrations.Counter
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
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
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(ReadCounter))]
        public static async Task<IActionResult> ReadCounter(
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
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(DeleteCounter))]
        public static async Task<IActionResult> DeleteCounter(
            [HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "counter/{key}")] HttpRequest req,
            string key,
            [DurableClient] IDurableClient client)
        {
            try
            {
                var entityId = new EntityId("Counter", key);
                await client.SignalEntityAsync(entityId, "delete");
                return new OkObjectResult($"delete was sent to {entityId}.\n");
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(CrashCounter))]
        public static async Task<IActionResult> CrashCounter(
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
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }

        [FunctionName(nameof(CountSignals))]
        public static async Task<IActionResult> CountSignals(
             [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "signal-counter/{count}")] HttpRequest req,
             int count,
             [DurableClient] IDurableClient client)
        {
            try
            {
                string key = Guid.NewGuid().ToString("N");
                var entityId = new EntityId("Counter", key);

                DateTime startTime = DateTime.UtcNow;

                // send the specified number of signals to the counter entity
                // for better send throughput we do not send signals one at a time, but use parallel tasks
                await Enumerable.Range(0, count).ParallelForEachAsync(50, true, i => client.SignalEntityAsync(entityId, "add", 1));

                // poll the entity until the expected count is reached
                while ((DateTime.UtcNow - startTime) < TimeSpan.FromMinutes(3))
                {
                    var response = await client.ReadEntityStateAsync<Counter>(entityId);

                    if (response.EntityExists
                        && response.EntityState.CurrentValue == count)
                    {
                        var state = response.EntityState;
                        var elapsedSeconds = (state.LastModified - state.StartTime.Value).TotalSeconds;
                        var throughput = count / elapsedSeconds;
                        return new OkObjectResult(new { elapsedSeconds, count, throughput });
                    }

                    await Task.Delay(TimeSpan.FromSeconds(5));
                }

                return new OkObjectResult($"timed out after {(DateTime.UtcNow - startTime)}.\n") { StatusCode = (int)HttpStatusCode.RequestTimeout };
            }
            catch (Exception e)
            {
                return new ObjectResult(e.ToString()) { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }
    }
}