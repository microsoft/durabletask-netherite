// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
    /// It returns the result of the orchestration, the time it started, and the duration.
    /// </summary>
    public static class GenericHttp
    {
        /// <summary>
        /// The JSON object included in the request body.
        /// </summary>
        [JsonObject]
        public struct Arguments
        {
            /// <summary>
            /// The name of the orchestration.
            /// </summary>
            public string Name { get; set; }

            /// <summary>
            /// An instance id for the orchestration, or null if not specified.
            /// </summary>
            public string InstanceId { get; set; }

            /// <summary>
            /// The input for the orchestration.
            /// </summary>
            public JToken Input { get; set; }

            /// <summary>
            /// A timeout. If the orchestration takes longer, the request returns with a 202 response.
            /// </summary>
            public int Timeout { get; set; }

            /// <summary>
            /// If true, use the internally recorded start and end time. Otherwise, measure wall clock time.
            /// </summary>
            public bool UseReportedLatency { get; set; }
        }

        /// <summary>
        /// The JSON object returned in the response body.
        /// </summary>
        [JsonObject]
        public struct Response
        {
            /// <summary>
            /// The result returned by the orchestration.
            /// </summary>
            public string Result { get; set; }

            /// <summary>
            /// The UTC time at which the orchestration started.
            /// </summary>
            public DateTime Time { get; set; }
            
            /// <summary>
            /// The duration of the orchestration in milliseconds.
            /// </summary>
            public double Duration { get; set; }
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
                    // issue start and wait together. This can be a bit faster for orchestrations that complete very quickly.
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
                    response = new OkObjectResult(new Response {
                        Result = (string)JToken.Parse(await stringContent.ReadAsStringAsync()),
                        Time = endtime, 
                        Duration = (endtime - starttime).TotalMilliseconds 
                    });
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