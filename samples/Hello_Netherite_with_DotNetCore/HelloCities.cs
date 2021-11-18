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
    /// A simple durable orchestration that calls three activities in a sequence.
    /// </summary>
    public static class HelloCities
    {
        [FunctionName(nameof(HelloCities))]
        public async static Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "hellocities")] HttpRequest req,
            [DurableClient] IDurableClient client)
        {
            string orchestrationInstanceId = await client.StartNewAsync(nameof(HelloSequence));
            TimeSpan timeout = TimeSpan.FromSeconds(30);
            return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, timeout);
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