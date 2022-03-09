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
    using System.Net.Http;
    using DurableTask.Core.Stats;

    /// <summary>
    /// Http triggers for operations on large numbers of orchestration instances
    /// 
    /// Example curl invocations:
    ///     curl https://.../start ...               launch instances with prefix "Orch"
    ///     curl https://.../many/abc/start ..       launch instances with prefix "abc"
    ///     
    /// </summary>
    public static class ManyOrchestrationsHttp
    {
        [FunctionName(nameof(Start))]
        public static Task<IActionResult> Start(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            return ManyOrchestrations.Start(req, client, log, "Orch");
        }

        [FunctionName(nameof(Await))]
        public static Task<IActionResult> Await(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            return ManyOrchestrations.Await(req, client, log, "Orch");
        }

        [FunctionName(nameof(Count))]
        public static Task<IActionResult> Count(
          [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "count")] HttpRequest req,
          [DurableClient] IDurableClient client,
          ILogger log)
        {
            return ManyOrchestrations.Count(req, client, log, "Orch");
        }

        [FunctionName(nameof(Query))]
        public static Task<IActionResult> Query(
          [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequest req,
          [DurableClient] IDurableClient client,
          ILogger log)
        {
            return ManyOrchestrations.Query(req, client, log, "Orch");
        }

        [FunctionName(nameof(Purge))]
        public static Task<IActionResult> Purge(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            return ManyOrchestrations.Purge(req, client, log, "Orch");
        }

        [FunctionName(nameof(ManyOrchestrationsPost))]
        public static async Task<IActionResult> ManyOrchestrationsPost(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "many/{prefix}/{action}")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log,
            string prefix,
            string action)
        {
            switch (action)
            {
                case "start":
                    return await ManyOrchestrations.Start(req, client, log, prefix);
                case "purge":
                    return await ManyOrchestrations.Purge(req, client, log, prefix);
                case "count":
                    return await ManyOrchestrations.Count(req, client, log, prefix);
                case "await":
                    return await ManyOrchestrations.Await(req, client, log, prefix);
                default:
                    return new NotFoundObjectResult($"no such action: POST ${action}");
            }
        }

        [FunctionName(nameof(ManyOrchestrationsGet))]
        public static async Task<IActionResult> ManyOrchestrationsGet(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "many/{prefix}/{action}")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log,
            string prefix,
            string action)
        {
            switch (action)
            {
                case "query":
                    return await ManyOrchestrations.Query(req, client, log, prefix);
                default:
                    return new NotFoundObjectResult($"no such action: GET ${action}");
            }
        }
    }
}
