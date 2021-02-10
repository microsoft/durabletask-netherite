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
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using System.Collections.Generic;
    using System.Net.Http;
    using Newtonsoft.Json.Linq;
    using System.Linq;

    /// <summary>
    /// A very simple Http Trigger that is useful for testing whether the orchestration service has started correctly, and what its
    /// basic configuration is.
    /// </summary>
    public static class Ping
    {
        [FunctionName(nameof(Ping))]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            log.LogInformation("C# HTTP trigger function processed a request.");
            var is64bit = Environment.Is64BitProcess;

            if (int.TryParse(requestBody, out int numPartitionsToPing))
            {
                async Task<string> Ping(int partition)
                {
                    string instanceId = $"ping!{partition:D2}";
                    await client.StartNewAsync(nameof(Pingee), instanceId);
                    var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, instanceId, TimeSpan.FromSeconds(30));
                    if (response is ObjectResult objectResult
                      && objectResult.Value is HttpResponseMessage responseMessage
                      && responseMessage.StatusCode == System.Net.HttpStatusCode.OK
                      && responseMessage.Content is StringContent stringContent)
                    {
                        return await stringContent.ReadAsStringAsync();
                    }
                    else
                    {
                        return "timeout";
                    }
                }

                var tasks = Enumerable.Range(0, numPartitionsToPing).Select(partition => Ping(partition)).ToList();

                await Task.WhenAll(tasks);

                JObject result = new JObject();
                var distinct = new HashSet<string>();
                for (int i = 0; i < tasks.Count; i++)
                {
                    result.Add(i.ToString(), tasks[i].Result);
                    distinct.Add(tasks[i].Result);
                }
                result.Add("distinct", distinct.Count);

                return new OkObjectResult(result.ToString());
            }
            else
            {
                return new OkObjectResult($"Hello from {client} ({(is64bit ? "x64" : "x32")})\n");
            }
        }


        [FunctionName(nameof(Pingee))]
        public static Task<string> Pingee([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            return Task.FromResult(Environment.MachineName);
        }
    }
}
