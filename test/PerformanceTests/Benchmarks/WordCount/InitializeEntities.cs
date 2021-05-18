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

    public static class InitializeEntities
    {        
        [FunctionName(nameof(InitializeEntities))]
        public static async Task Run(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger logger)
        {
            var (mapperCount, reducerCount) = context.GetInput<(int, int)>();

            var tasks = new List<Task>();

            // initialize all the mapper entities
            for (int i = 0; i < mapperCount; i++)
            {
                tasks.Add(context.CallEntityAsync(Mapper.GetEntityId(i), nameof(Mapper.Ops.Init), reducerCount.ToString()));
            }

            // initialize all the reducer entities
            for (int i = 0; i < reducerCount; i++)
            {
                tasks.Add(context.CallEntityAsync(Reducer.GetEntityId(i), nameof(Reducer.Ops.Init), mapperCount.ToString()));
            }

            // initialize the summary entity
            tasks.Add(context.CallEntityAsync(Summary.GetEntityId(), nameof(Summary.Ops.Init), reducerCount.ToString()));

            // we want to have the initialization completed before we start the test
            await Task.WhenAll(tasks);
        }
    }
}