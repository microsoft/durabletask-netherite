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
    public static class HelloCitiesSequence
    {
        [FunctionName(nameof(HelloCities))]
        public static async Task<IActionResult> HelloCities(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            // start the orchestration
            string orchestrationInstanceId = await client.StartNewAsync(nameof(HelloSequence));

            // wait for it to complete and return the result
            return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(200));
        }

        [FunctionName(nameof(HelloCities5))]
        public static async Task<IActionResult> HelloCities5(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            // start the orchestration
            string orchestrationInstanceId = await client.StartNewAsync(nameof(HelloSequence5));

            // wait for it to complete and return the result
            return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromSeconds(200));
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

        [FunctionName(nameof(HelloSequence5))]
        public static async Task<List<string>> HelloSequence5([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var outputs = new List<string>
            {
                await context.CallActivityAsync<string>(nameof(SayHello), "Tokyo"),
                await context.CallActivityAsync<string>(nameof(SayHello), "Seattle"),
                await context.CallActivityAsync<string>(nameof(SayHello), "London"),
                await context.CallActivityAsync<string>(nameof(SayHello), "Amsterdam"),
                await context.CallActivityAsync<string>(nameof(SayHello), "Mumbai")
            };

            return outputs;
        }

        [FunctionName(nameof(SayHello))]
        public static string SayHello([ActivityTrigger] string name) => $"Hello {name}!";
    }
}
