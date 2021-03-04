// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.WordCount
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

    public static class Reducer
    {
        public static EntityId GetEntityId(int number)
        {
            return new EntityId(nameof(Reducer), $"{number}!{number % 100:D2}");
        }

        public enum Ops
        {
            Init,
            Inc,
            MapperEnd,
            Read,
        }

        public class ReducerState
        {
            public int mapperCount;
            public int countOfCompletedMappers;
            public int entryCount;
            public Dictionary<string, int> wordCount;
            public DateTime completionTime;
        }

        public class Result
        {
            public int entryCount;
            public Dictionary<string, int> top;
        }

        [FunctionName(nameof(Reducer))]
        public static Task HandleOperation(
            [EntityTrigger] IDurableEntityContext context, ILogger log)
        {
            char[] separators = { ' ', '\n', '<', '>', '=', '\"', '\'', '/', '\\', '(', ')', '\t', '{', '}', '[', ']', ',', '.', ':', ';' };

            var state = context.GetState(() => new ReducerState());

            switch (Enum.Parse<Ops>(context.OperationName))
            {
                case Ops.Init:
                    if (!int.TryParse(context.GetInput<string>(), out var mapperCount))
                    {
                        log.LogError($"{context.EntityId}: Error Parsing count {context.GetInput<string>()} into integer");
                    }
                    else
                    {
                        state.mapperCount = mapperCount;
                        state.countOfCompletedMappers = 0;
                        state.wordCount = new Dictionary<string, int>();
                        state.entryCount = 0;
                        log.LogWarning($"{context.EntityId}: initialized, mapper count <- {mapperCount}");
                    }
                    break;

                case Ops.Inc:
                    state.entryCount++;
                    var word = context.GetInput<string>();
                    if (!state.wordCount.ContainsKey(word))
                    {
                        state.wordCount[word] = 1;
                    }
                    else
                    {
                        state.wordCount[word]++;
                    }
                    if (state.entryCount % 1000 == 0)
                    {
                        // progress report
                        log.LogWarning($"{context.EntityId}: processed {state.entryCount} words");
                    }
                    break;

                case Ops.MapperEnd:
                    ++state.countOfCompletedMappers;
                    if (state.countOfCompletedMappers >= state.mapperCount)
                    {
                        context.SignalEntity(Summary.GetEntityId(), nameof(Summary.Ops.Report), new Summary.Report()
                        {
                            entryCount = state.entryCount,
                            topWords = state.wordCount.OrderByDescending(kvp => kvp.Value).Take(20).Select(kvp => (kvp.Value,kvp.Key)).ToList(),
                        });
                    }
                    break;
            }
            return Task.CompletedTask;
        }
    }
}
