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
           [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "periodic/{iterations}/{minutes}/")] HttpRequest req,
           [DurableClient] IDurableClient client,
           int iterations,
           double minutes,
           ILogger log)
        {
            // start the orchestration
            string orchestrationInstanceId = await client.StartNewAsync(nameof(PeriodicOrchestration), null, (iterations, minutes));

            return client.CreateCheckStatusResponse(req, orchestrationInstanceId, false);
        }
    }
}