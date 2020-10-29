// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace PerformanceTests
{
    using System;
    using System.IO;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// A generic HttpTrigger wrapper that can be used to launch an orchestration.
    /// </summary>
    public static class GenericHttp
    {
        [JsonObject]
        public struct Arguments
        {
            public string Name { get; set; }

            public string InstanceId { get; set; }

            public JToken Input { get; set; }

            public int Timeout { get; set; }

            public bool UseReportedLatency { get; set; }
        }

        [FunctionName(nameof(GenericHttp))]
        public static async Task<IActionResult> Run(
          [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "genericHttp")] HttpRequest req,
          [DurableClient] IDurableClient client,
          ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            Arguments arguments = JsonConvert.DeserializeObject<Arguments>(requestBody);
            IActionResult response;
            try
            {
                DateTime starttime = DateTime.UtcNow;

                if (arguments.InstanceId == null)
                {
                    // start the orchestration, and wait for the persistence confirmation
                    arguments.InstanceId = await client.StartNewAsync<JToken>(arguments.Name, null, arguments.Input);
                    // then wait for completion
                    response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, arguments.InstanceId, TimeSpan.FromSeconds(arguments.Timeout));
                }
                else
                {
                    // issue start and wait together
                    Task start = client.StartNewAsync<JToken>(arguments.Name, arguments.InstanceId, arguments.Input);
                    Task<IActionResult> completion = client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, arguments.InstanceId, TimeSpan.FromSeconds(arguments.Timeout));
                    await start;
                    response = await completion;
                }

                DateTime endtime = DateTime.UtcNow;

                if (response is ObjectResult objectResult
                    && objectResult.Value is HttpResponseMessage responseMessage
                    && responseMessage.StatusCode == System.Net.HttpStatusCode.OK
                    && responseMessage.Content is StringContent stringContent)
                {
                    if (arguments.UseReportedLatency)
                    {
                        var state = await client.GetStatusAsync(arguments.InstanceId, false, false, false);
                        starttime = state.CreatedTime;
                        endtime = state.LastUpdatedTime;
                    }
                    response = new OkObjectResult(new { Result = await stringContent.ReadAsStringAsync(), Time = endtime, Duration = (endtime - starttime).TotalMilliseconds });
                }
                else
                {
                    return response; // it is a 202 response that will be interpreted as a timeout
                }
            }
            catch (Exception e)
            {
                response = new System.Web.Http.ExceptionResult(e, true);
            }
            return response;
        }
    }
}