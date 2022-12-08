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
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;

    /// <summary>
    /// An activity that 
    /// </summary>
    public static class HashActivity
    {
        [FunctionName(nameof(HashActivity))]
        public static async Task<long> Run([ActivityTrigger] IDurableActivityContext context)
        {
            var input = context.GetInput<(string book, int multiplier)>();

            // download the book content from the corpus
            var storageConnectionString = Environment.GetEnvironmentVariable("CorpusConnection");
            var blobClient = new BlobClient(storageConnectionString, blobContainerName: "gutenberg", blobName: input.book);
            BlobDownloadResult result = await blobClient.DownloadContentAsync();
            string doc = result.Content.ToString();

            long wordCount = 0;
            string[] words = doc.Split(separators, StringSplitOptions.RemoveEmptyEntries);

            // randomly scale up the work to create unbalanced work between activities
            for (int i = 0; i < input.multiplier; i++)
            { 
                foreach (string word in words)
                {
                    int v = word.GetHashCode();
                    wordCount++;
                }
            }

            // return the number of words hashed
            return wordCount;
        }

        static readonly char[] separators = { ' ', '\n', '<', '>', '=', '\"', '\'', '/', '\\', '(', ')', '\t', '{', '}', '[', ']', ',', '.', ':', ';' };
    }
}
