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

        [FunctionName(nameof(PeriodicOrchestration))]
        public static async Task PeriodicOrchestration([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            for (int i = 0; i < 5; i++)
            {
                DateTime fireAt = context.CurrentUtcDateTime + TimeSpan.FromSeconds(60);

                if (!context.IsReplaying)
                {
                    log.LogWarning($"{context.InstanceId}: starting timer for iteration {i}, to fire at {fireAt}");
                }

                await context.CreateTimer(fireAt, CancellationToken.None);

                if (!context.IsReplaying)
                {
                    log.LogWarning($"{context.InstanceId}: timer for iteration {i} fired at {(DateTime.UtcNow - fireAt).TotalMilliseconds:F2}ms relative to deadline");
                }
            };
        }
    }
}