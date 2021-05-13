// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Periodic
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Logging;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

    /// <summary>
    /// A simple microbenchmark orchestration that some trivial activities in a sequence.
    /// </summary>
    public static class Periodic
    {
        [FunctionName(nameof(Periodic))]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            // start the orchestration
            string orchestrationInstanceId = await client.StartNewAsync(nameof(PeriodicOrchestration));

            // wait for it to complete and return the result
            return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(200));
        }   
    }
}