// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.WordCount
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Net;
    using System.Threading.Tasks;
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
            return new EntityId(nameof(Mapper), $"{number}!{number % 100:D2}");
        }

        [FunctionName(nameof(Mapper))]
        public static Task HandleOperation(
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
                        log.LogInformation($"{context.EntityId}: initialized, reducer count <- {reducerCount}");
                    }
                    context.SetState(reducerCount);
                    break;

                case Ops.Item:
                    {
                        log.LogInformation($"{context.EntityId}: Start processing.");
                        Stopwatch s = new Stopwatch();
                        s.Start();
                        string documentUrl = context.GetInput<string>();
                        string doc = (new WebClient()).DownloadString(documentUrl);
                        string[] words = doc.Split(separators, StringSplitOptions.RemoveEmptyEntries);
                        foreach (var word in words)
                        {
                            int hash = word.GetHashCode();
                            if (hash < 0)
                            {
                                hash = -hash;
                            }
                            int reducerNumber = hash % reducerCount;
                            context.SignalEntity(Reducer.GetEntityId(reducerNumber), nameof(Reducer.Ops.Inc), word);
                        }
                        s.Stop();
                        log.LogInformation($"{context.EntityId}: Downloaded and processed {words.Length} words in {s.Elapsed.TotalSeconds:F3}s from {documentUrl}");
                        break;
                    }

                case Ops.End:
                    log.LogInformation($"{context.EntityId}: Forwarding end signal.");
                    for (int i = 0; i < reducerCount; i++)
                    {
                        context.SignalEntity(Reducer.GetEntityId(i), nameof(Reducer.Ops.MapperEnd));
                    }
                    break;

            }
            return Task.CompletedTask;
        }
    }
}