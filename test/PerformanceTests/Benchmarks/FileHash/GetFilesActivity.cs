// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.FileHash
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using System.Linq;
    using System.Collections.Generic;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// An activity that 
    /// </summary>
    public static class GetFilesActivity
    {
        [FunctionName(nameof(GetFilesActivity))]
        public static Task<List<string>> Run([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {

            // setup connection to the blob storage 
            string connectionString = Environment.GetEnvironmentVariable("CorpusConnection");
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient serviceClient = cloudStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer blobContainer = serviceClient.GetContainerReference("gutenberg");
            CloudBlobDirectory blobDirectory = blobContainer.GetDirectoryReference($"Gutenberg/txt");

            // get the list of files(books) from blob storage
            List<IListBlobItem> books = blobDirectory.ListBlobs().ToList();
            List<string> bookNames = new List<string>(); ;
            foreach (int _ in Enumerable.Range(1, 10))
            {
                bookNames = bookNames.Concat(books.Select(x => ((CloudBlockBlob)x).Name).ToList()).ToList();
            }
            log.LogWarning($"{bookNames.Count} books in total");

            return Task.FromResult(bookNames);
        }
    }
}
