// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.FileHash
{
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging;
    using System.Collections.Generic;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using System.Linq;

    /// <summary>
    /// An orchestration that 
    /// </summary>
    public static class FileOrchestration
    {

        [FunctionName(nameof(FileOrchestration))]
        public static async Task<double> Run([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            // get the input
            int numFiles = context.GetInput<int>();

            var books = await context.CallActivityAsync<List<string>>(nameof(GetFilesActivity), null);

            int fileCount = 0;
            var results = new List<Task<long>>();

            foreach (var book in books)
            {
                Task<long> wordCount = context.CallActivityAsync<long>(nameof(HashActivity), book);
                //log.LogWarning($"Processing {book}");

                results.Add(wordCount);
                fileCount++;

                if (fileCount == numFiles)
                {
                    log.LogWarning($"Sent request to process {fileCount} files, exiting");
                    break;
                }
            }

            await Task.WhenAll(results);
            double sum = (double)results.Sum(t => t.Result) / 1000000;
            return sum;
        }
    }
}


