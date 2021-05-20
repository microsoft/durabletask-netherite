// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.FanOutFanIn
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
    /// A simple microbenchmark orchestration that executes activities in parallel.
    /// </summary>
    public static class HttpTriggers
    {
        [FunctionName(nameof(FanOutFanIn))]
        public static async Task<IActionResult> RunFanOutFanIn(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "fanoutfanin")] HttpRequest req,
           [DurableClient] IDurableClient client)
        {
            // get the number of tasks
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            int count = int.Parse(requestBody);

            // start the orchestration
            string orchestrationInstanceId = await client.StartNewAsync(nameof(FanOutFanInOrchestration), null, count);

            // wait for it to complete and return the result
            return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(200));
        }
    }
}
