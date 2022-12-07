// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.FileHash
{
    using System;
    using System.Threading.Tasks;
    using System.Linq;
    using System.Collections.Generic;
    using Microsoft.Extensions.Logging;
    using Azure.Storage.Blobs;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

    /// <summary>
    /// An activity that 
    /// </summary>
    public static class GetFilesActivity
    {
        [FunctionName(nameof(GetFilesActivity))]
        public static async Task<List<string>> Run([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            // list all the books in the corpus
            var storageConnectionString = Environment.GetEnvironmentVariable("CorpusConnection");
            var blobContainerClient = new BlobContainerClient(storageConnectionString, blobContainerName: "gutenberg");

            List<string> bookNames = new List<string>();
            await foreach (var blob in blobContainerClient.GetBlobsAsync(prefix: "Gutenberg/txt"))
            {
                bookNames.Add(blob.Name);
            }
            log.LogWarning($"{bookNames.Count} books in total");

            return  bookNames;
        }
    }
}
