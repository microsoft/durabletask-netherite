// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Sequence
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
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.WindowsAzure.Storage.Queue;

    /// <summary>
    /// A microbenchmark that runs a sequence of tasks. The point is to compare sequence construction using blob-triggers
    /// with an orchestrator doing the same thing.
    /// 
    /// The blob and queue triggers are disabled by default. Uncomment the [Disable] attribute before compilation to use them.
    /// </summary>
    public static class HttpTriggers
    {
        [Disable]
        [FunctionName(nameof(RunOrchestratedSequence))]
        public static async Task<IActionResult> RunOrchestratedSequence(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "RunOrchestratedSequence")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            TimeSpan timeout = TimeSpan.FromSeconds(200);
            string orchestrationInstanceId = await client.StartNewAsync(nameof(OrchestratedSequence), null, new Sequence.Input()
            {
                Length = 10,
                WorkExponent = 0,
                DataExponent = 0,
            });
            var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, timeout);
            return response;
        }

        [Disable]
        [FunctionName(nameof(RunBlobSequence))]
        public static async Task<IActionResult> RunBlobSequence(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "RunBlobSequence")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            TimeSpan timeout = TimeSpan.FromSeconds(200);
            string orchestrationInstanceId = await client.StartNewAsync(nameof(BlobSequence), null, new Sequence.Input()
            {
                Length = 10,
                WorkExponent = 0,
                DataExponent = 0,
            });
            var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, timeout);
            return response;
        }

        [Disable]
        [FunctionName(nameof(RunQueueSequence))]
        public static async Task<IActionResult> RunQueueSequence(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "RunQueueSequence")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            TimeSpan timeout = TimeSpan.FromSeconds(200);
            string orchestrationInstanceId = await client.StartNewAsync(nameof(QueueSequence), null, new Sequence.Input()
            {
                Length = 10,
                WorkExponent = 0,
                DataExponent = 0,
            });
            var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, timeout);
            return response;
        }
    }
}