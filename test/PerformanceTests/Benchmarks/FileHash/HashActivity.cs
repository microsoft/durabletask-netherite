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

    /// <summary>
    /// An activity that 
    /// </summary>
    public static class HashActivity
    {
        [FunctionName(nameof(HashActivity))]
        public static async Task<long> Run([ActivityTrigger] IDurableActivityContext context)
        {
            char[] separators = { ' ', '\n', '<', '>', '=', '\"', '\'', '/', '\\', '(', ')', '\t', '{', '}', '[', ']', ',', '.', ':', ';' };

            // setup connection to the blob storage
            string connectionString = Environment.GetEnvironmentVariable("CorpusConnection");
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient serviceClient = cloudStorageAccount.CreateCloudBlobClient();

            // download the book from blob storage
            var input = context.GetInput<(string book, int multiplier)>();
            CloudBlobContainer blobContainer = serviceClient.GetContainerReference("gutenberg");
            CloudBlockBlob blob = blobContainer.GetBlockBlobReference(input.book);
            string doc = await blob.DownloadTextAsync();

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
    }
}
