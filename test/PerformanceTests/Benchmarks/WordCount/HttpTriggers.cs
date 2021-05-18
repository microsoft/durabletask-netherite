// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.WordCount
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
    using Microsoft.Azure.Storage;
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Azure.Storage.Blob;
    using DurableTask.Netherite.Faster;
    using System.Net.Http;

    /// <summary>
    /// Defines the REST operations for the word count test.
    /// </summary>
    public static class HttpTriggers
    {
        [FunctionName("Wordcount")]
        public static async Task<IActionResult> Run(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "wordcount")] HttpRequest req,
           [DurableClient] IDurableClient client,
           ILogger log)
        {
            log.LogWarning($"request is {req}");
            var queryParameters = req.Query;

            // get mapper and reducer count from shape parameter
            string shape = req.Query["shape"];
            string[] counts = shape.Split('x');
            int mapperCount, reducerCount;

            // Get the number of books to process
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            int maxBooks = int.Parse(requestBody);

            // setup connection to the blob storage
            string connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            // TODO: Add connection string as an argument
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient serviceClient = cloudStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer blobContainer = serviceClient.GetContainerReference("gutenberg");
            CloudBlobDirectory blobDirectory = blobContainer.GetDirectoryReference($"Gutenberg/txt");
            // CloudBlobDirectory blobDirectory = blobContainer.GetDirectoryReference($"Gutenberg-small");

            // get the list of files(books) from blob storage
            IEnumerable<IListBlobItem> books = blobDirectory.ListBlobs();

            if (!(counts.Length == 2 && int.TryParse(counts[0], out mapperCount) && int.TryParse(counts[1], out reducerCount)))
            {
                return new BadRequestObjectResult("Please specify the mapper count and reducer count in the query parameters,  e.g. &shape=10x10");
            }

            // ----- PHASE 1 ----------
            // initialize all three types of entities prior to running the mapreduce

            var initializationOrchestrationInstanceId = await client.StartNewAsync(nameof(InitializeEntities), null, (mapperCount, reducerCount));
            var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, initializationOrchestrationInstanceId, TimeSpan.FromMinutes(3));
            if (! (response is ObjectResult objectResult
                    && objectResult.Value is HttpResponseMessage responseMessage
                    && responseMessage.StatusCode == System.Net.HttpStatusCode.OK))
            {
                return (ActionResult)new OkObjectResult($"Initialization orchestration timed out.\n");
            }

            // ----- PHASE 2 ----------
            // send work to the mappers

            int bookCount = 0;

            foreach (var book in books)
            {
                log.LogWarning($"The book name is {((CloudBlockBlob)book).Name}");
                int mapper = bookCount++ % mapperCount;
                var _ = client.SignalEntityAsync(Mapper.GetEntityId(mapper), nameof(Mapper.Ops.Item), ((CloudBlockBlob)book).Name);
                
                if (bookCount == maxBooks)
                {
                    log.LogWarning($"Processed {bookCount}, exiting");
                    break;
                }
            }

            for (int i = 0; i < mapperCount; i++)
            {
                var _ = client.SignalEntityAsync(Mapper.GetEntityId(i), nameof(Mapper.Ops.End));
            }

            // ----- PHASE 3 ----------
            // wait for summary entity to contain the final result

            var startWaitingAt = DateTime.UtcNow;
            int entryCount = 0;
            List<(int, string)> topWords = null;
            double executionTime = 0;
            do
            {
                await Task.Delay(500);
                var summaryState = await client.ReadEntityStateAsync<Summary.SummaryState>(Summary.GetEntityId());
                if (summaryState.EntityExists && summaryState.EntityState.waitCount == 0)
                {
                    entryCount = summaryState.EntityState.entryCount;
                    topWords = summaryState.EntityState.topWords;
                    executionTime = summaryState.EntityState.executionTimeInSeconds;
                    break;
                }
            }
            while (DateTime.UtcNow < startWaitingAt + TimeSpan.FromMinutes(5));

            if (topWords == null)
            {
                return (ActionResult)new OkObjectResult($"Timed out.\n");
            }
            else
            {
                var sb = new StringBuilder();
                sb.AppendLine($"----- {bookCount} books with {entryCount} words processed in {executionTime:F2}s, top 20 as follows -----");
                foreach ((int count, string word) in topWords)
                {
                    sb.AppendLine($"{count,10} {word}");
                }
                sb.AppendLine($"----- Throughput is {entryCount/executionTime:F2} -----");
                return (ActionResult)new OkObjectResult(sb.ToString());
            }
        }
    }
}