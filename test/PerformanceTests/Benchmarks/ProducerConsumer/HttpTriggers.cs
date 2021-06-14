// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.ProducerConsumer
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

    public static class HttpTriggers
    {
        [FunctionName("ProducerConsumer")]
        public static async Task<IActionResult> ProducerConsumer(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "producerconsumer")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            try
            {
                string request = await new StreamReader(req.Body).ReadToEndAsync();

                Parameters parameters;

                switch (request)
                {
                    case "minimal":
                        parameters = new Parameters()
                        {
                            producers = 1,
                            producerPartitions = 1,
                            consumers = 1,
                            consumerPartitions = 1,
                            batches = 1,
                            batchsize = 1,
                            messagesize = 0,
                        };
                        break;

                    case "pipe-10x300x1k":
                        parameters = new Parameters()
                        {
                            producers = 1,
                            producerPartitions = 1,
                            consumers = 1,
                            consumerPartitions = 1,
                            batches = 10,
                            batchsize = 300,
                            messagesize = 1000,
                        };
                        break;

                    case "pipe-10x3x100k":
                        parameters = new Parameters()
                        {
                            producers = 1,
                            producerPartitions = 1,
                            consumers = 1,
                            consumerPartitions = 1,
                            batches = 10,
                            batchsize = 3,
                            messagesize = 100000,
                        };
                        break;

                    case "pipe-1x3000x1k":
                        parameters = new Parameters()
                        {
                            producers = 1,
                            producerPartitions = 1,
                            consumers = 1,
                            consumerPartitions = 1,
                            batches = 1,
                            batchsize = 3000,
                            messagesize = 1000,
                        };
                        break;

                    case "debug":
                        parameters = new Parameters()
                        {
                            producers = 10,
                            producerPartitions = 2,
                            consumers = 10,
                            consumerPartitions = 2,
                            batches = 3,
                            batchsize = 100,
                            messagesize = 1000,
                        };
                        break;

                    default:
                        parameters = JsonConvert.DeserializeObject<Parameters>(request);
                        break;
                }

                if (string.IsNullOrEmpty(parameters.testname))
                {
                    parameters.testname = string.Format($"producerconsumer{Guid.NewGuid().ToString("N").Substring(0,5)}");
                }

                IActionResult result;

                // initialize and start the shuffle
                int partitionId = parameters.producerPartitions + parameters.consumerPartitions;
                string instanceId = $"{parameters.testname}!{partitionId:D2}";
                string orchestrationInstanceId = await client.StartNewAsync(nameof(ProducerConsumerOrchestration), instanceId, parameters);
                result = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, orchestrationInstanceId, TimeSpan.FromMinutes(5));
                return result;
            }
            catch (Exception e)
            {
                return new ObjectResult(
                    new
                    {
                        error = e.ToString(),
                    });
            }
        }
    }
}
