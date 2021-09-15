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

            var results = new List<Task<long>>();
            System.Random random = new System.Random(0);

            for (int i = 0; i < numFiles; i++)
            {
                (string book, int multiplier) input = (books[i % books.Count], random.Next(20, 50));
                results.Add(context.CallActivityAsync<long>(nameof(HashActivity), input));
            }

            await Task.WhenAll(results);
            double sum = (double)results.Sum(t => t.Result) / 1000000;
            return sum;
        }
    }
}


