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
            // setup connection to the corpus with the text files 
            CloudBlobClient serviceClient = new CloudBlobClient(new Uri(@"https://gutenbergcorpus.blob.core.windows.net")); 
            CloudBlobContainer blobContainer = serviceClient.GetContainerReference("gutenberg");
            CloudBlobDirectory blobDirectory = blobContainer.GetDirectoryReference($"Gutenberg/txt");

            // get the list of files(books) from blob storage
            List<IListBlobItem> books = blobDirectory.ListBlobs().ToList();
            List<string> bookNames = books.Select(x => ((CloudBlockBlob)x).Name).ToList();
            log.LogWarning($"{bookNames.Count} books in total");
            return Task.FromResult(bookNames);
        }
    }
}
