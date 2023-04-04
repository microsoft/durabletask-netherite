// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.HelloCities
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
    /// A simple microbenchmark orchestration that some trivial activities in a sequence.
    /// </summary>
    public static class SemaphoreHttpTriggers
    {
        [FunctionName(nameof(SemaphoreOrchestration))]
        public static async Task<IActionResult> SemaphoreOrchestration(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            // start the orchestration
            string orchestrationInstanceId = await client.StartNewAsync("OrchestrationWithSemaphore");

            // wait for it to complete and return the result
            return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(400));
        }

        [FunctionName(nameof(Semaphore))]
        public static async Task<IActionResult> Semaphore(
          [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequest req,
          [DurableClient] IDurableClient client,
          ILogger log)
        {
            var response = await client.ReadEntityStateAsync<SemaphoreTest.SemaphoreEntity>(new EntityId("SemaphoreEntity", "MySemaphoreInstance"));
            return new OkObjectResult(response);
        }
    }
}
