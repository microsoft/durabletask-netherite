// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.WordCount
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Faster;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

    public static class Mapper
    {

        public enum Ops
        {
            Init,
            Item,
            End,
        }

        public static EntityId GetEntityId(int number)
        {
            return new EntityId(nameof(Mapper), $"!{number}");
        }

        [FunctionName(nameof(Mapper))]
        public static async Task HandleOperation(
            [EntityTrigger] IDurableEntityContext context, ILogger log)
        {
            char[] separators = { ' ', '\n', '<', '>', '=', '\"', '\'', '/', '\\', '(', ')', '\t', '{', '}', '[', ']', ',', '.', ':', ';' };

            // the only thing we remember is the count of the reducer.
            var reducerCount = context.GetState(() => 1000);

            switch (Enum.Parse<Ops>(context.OperationName))
            {
                case Ops.Init:
                    if (!int.TryParse(context.GetInput<string>(), out reducerCount))
                    {
                        log.LogError($"{context.EntityId}: Error Parsing count {context.GetInput<string>()} into integer");
                    }
                    else
                    {
                        log.LogWarning($"{context.EntityId}: initialized, reducer count <- {reducerCount}");
                    }
                    context.SetState(reducerCount);
                    break;

                case Ops.Item:
                    {
                        log.LogInformation($"{context.EntityId}: Start processing.");
                        Stopwatch s = new Stopwatch();
                        s.Start();

                        // setup connection to the corpus with the text files 
                        CloudBlobClient serviceClient = new CloudBlobClient(new Uri(@"https://gutenbergcorpus.blob.core.windows.net"));

                        // download the book from blob storage
                        string book = context.GetInput<string>();
                        CloudBlobContainer blobContainer = serviceClient.GetContainerReference("gutenberg");
                        CloudBlockBlob blob = blobContainer.GetBlockBlobReference(book);
                        string doc = await blob.DownloadTextAsync();

                        string[] words = doc.Split(separators, StringSplitOptions.RemoveEmptyEntries);

                        int wordsCounted = 0; 

                        foreach (var word in words)
                        {
                            int hash = word.GetHashCode();
                            if (hash < 0)
                            {
                                hash = -hash;
                            }
                            int reducerNumber = hash % reducerCount;
                            context.SignalEntity(Reducer.GetEntityId(reducerNumber), nameof(Reducer.Ops.Inc), word);

                            // some books are very large, causing extreme load imbalance when the overall number of books is small.
                            // for the sake of benchmarking, we limit the words counted in each book to 5000.
                            if (++wordsCounted == 5000)
                            {
                                break;
                            }
                        }

                        s.Stop();
                        log.LogWarning($"{context.EntityId}: Downloaded {words.Length} words, and counted {wordsCounted} of them, in {s.Elapsed.TotalSeconds:F3}s from {book}");
                        
                        log.LogWarning($"{context.EntityId}: Sending end signal to reducers.");
                        for (int i = 0; i < reducerCount; i++)
                        {
                            context.SignalEntity(Reducer.GetEntityId(i), nameof(Reducer.Ops.MapperEnd));
                        }
                        break;
                    }
            }
           
        }
    }
}