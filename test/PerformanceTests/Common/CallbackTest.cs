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
    using System.Net.Http;
    using System.Web.Http;

    public static class CallbackTest
    {
        [FunctionName(nameof(CallbackTest))]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "callbackTest")] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("CallbackTest function processed a request.");

            try
            {
                DateTime startTime = DateTime.UtcNow;
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                dynamic data = JsonConvert.DeserializeObject(requestBody);

                // extract the call back URL
                var callbackUri = (string)data.CallbackUri;

                var client = new HttpClient();

                // simulate random outcome
                int r = new Random().Next(10);
                await Task.Delay(10 * r);

                DateTime endTime = DateTime.UtcNow;

                dynamic callbackResponse = new
                {
                    StartTime = startTime,
                    EndTime = endTime,
                    CompletionTime = (double)(r * 10),
                };

                string responseString = JsonConvert.SerializeObject(callbackResponse);

                // post back the latency or error
                HttpResponseMessage message = await client.PostAsync(callbackUri, new StringContent(responseString));

                if (message.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    log.LogError($"Could not issue callback to {callbackUri}: {message.StatusCode} {message.ReasonPhrase}");

                    return new BadRequestErrorMessageResult($"Could not issue callback to {callbackUri}: {message.StatusCode} {message.ReasonPhrase}");
                }

                return new OkResult();
            }
            catch (Exception e)
            {
                return new BadRequestErrorMessageResult($"exception: {e}");
            }
        }
    }
}
