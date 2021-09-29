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
    using System.Net;

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
            // Get the mapper and reducer count from the shape
            string shape = await new StreamReader(req.Body).ReadToEndAsync();
            string[] counts = shape.Split('x');
            int mapperCount, reducerCount;

            // setup connection to the corpus with the text files 
            CloudBlobClient serviceClient = new CloudBlobClient(new Uri(@"https://gutenbergcorpus.blob.core.windows.net"));       
            CloudBlobContainer blobContainer = serviceClient.GetContainerReference("gutenberg");
            CloudBlobDirectory blobDirectory = blobContainer.GetDirectoryReference($"Gutenberg/txt");

            // get the list of files(books) from blob storage
            IEnumerable<IListBlobItem> books = blobDirectory.ListBlobs();

            if (!(counts.Length == 2 && int.TryParse(counts[0], out mapperCount) && int.TryParse(counts[1], out reducerCount)))
            {
                return new JsonResult(new { message = "Please specify the mapper count and reducer count in the query parameters,  e.g. &shape=10x10" })
                {
                    StatusCode = (int)HttpStatusCode.BadRequest
                };
            }

            // ----- PHASE 1 ----------
            // initialize all three types of entities prior to running the mapreduce

            var initializationOrchestrationInstanceId = await client.StartNewAsync(nameof(InitializeEntities), null, (mapperCount, reducerCount));
            var response = await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, initializationOrchestrationInstanceId, TimeSpan.FromMinutes(3));
            if (! (response is ObjectResult objectResult
                    && objectResult.Value is HttpResponseMessage responseMessage
                    && responseMessage.StatusCode == System.Net.HttpStatusCode.OK))
            {
                return new JsonResult(new { message = "Initialization orchestration timed out." })
                {
                    StatusCode = (int)HttpStatusCode.InternalServerError
                };
            }

            // ----- PHASE 2 ----------
            // send work to the mappers

            int bookCount = 0;

            foreach (var book in books)
            {
                log.LogWarning($"The book name is {((CloudBlockBlob)book).Name}");
                int mapper = bookCount++;
                var _ = client.SignalEntityAsync(Mapper.GetEntityId(mapper), nameof(Mapper.Ops.Item), ((CloudBlockBlob)book).Name);
                
                if (bookCount == mapperCount)
                {
                    log.LogWarning($"Processed {bookCount} books, exiting");
                    break;
                }
            }

            // ----- PHASE 3 ----------
            // wait for summary entity to contain the final result

            var startWaitingAt = DateTime.UtcNow;
            int size = 0;
            List<(int, string)> topWords = null;
            double elapsedSeconds = 0;
            double throughput = 0;
            do
            {
                await Task.Delay(10000);
                var summaryState = await client.ReadEntityStateAsync<Summary.SummaryState>(Summary.GetEntityId());
                if (summaryState.EntityExists && summaryState.EntityState.waitCount == 0)
                {
                    size = summaryState.EntityState.entryCount;
                    topWords = summaryState.EntityState.topWords;
                    elapsedSeconds = summaryState.EntityState.executionTimeInSeconds;
                    throughput = size / elapsedSeconds;
                    break;
                }
            }
            while (DateTime.UtcNow < startWaitingAt + TimeSpan.FromMinutes(5));

            if (topWords == null)
            {
                return new JsonResult(new { message = "Test timed out." })
                {
                    StatusCode = (int)HttpStatusCode.InternalServerError
                };
            }
            else
            {
                return new JsonResult(new {
                    bookCount,
                    size,
                    elapsedSeconds,
                    throughput,
                    //topWords,
                });              
            }
        }
    }
}