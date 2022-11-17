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
    using Newtonsoft.Json;
    using System.Net;

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
            log.LogWarning("Received ping {requestBody}", requestBody);
            var is64bit = Environment.Is64BitProcess;

            try
            {
                if (int.TryParse(requestBody, out int numPartitionsToPing))
                {
                    var timeout = TimeSpan.FromSeconds(15);

                    async Task<string> Ping(int partition)
                    {
                        string instanceId = $"ping!{partition}";

                        var timeoutTask = Task.Delay(timeout);
                        var startTask = client.StartNewAsync(nameof(Pingee), instanceId);
                        await Task.WhenAny(startTask, timeoutTask);
                        if (!startTask.IsCompleted)
                        {
                            return $"starting the orchestration timed out after {timeout}";
                        }
                        try
                        {
                            await startTask;
                        }
                        catch(InvalidOperationException)
                        {
                            // if orchestration already exists
                        }
                        var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, instanceId, timeout);
                        if (response is ObjectResult objectResult
                            && objectResult.Value is HttpResponseMessage responseMessage
                            && responseMessage.StatusCode == System.Net.HttpStatusCode.OK
                            && responseMessage.Content is StringContent stringContent)
                        {
                            return (string)JToken.Parse(await stringContent.ReadAsStringAsync());
                        }
                        return $"waiting for completion timed out after {timeout}";
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
            catch(Exception e)
            {
                return new ObjectResult($"exception: {e}") { StatusCode = (int)HttpStatusCode.InternalServerError };
            }
        }


        [FunctionName(nameof(Pingee))]
        public static Task<string> Pingee([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            return Task.FromResult(Environment.MachineName);
        }
    }
}
