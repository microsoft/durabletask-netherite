// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

    public class LauncherEntity
    {
        public void Launch((string orchestrationName, int numberOrchestrations, int offset, string input) input)
        {
            // start all the orchestrations
            for (int iteration = 0; iteration < input.numberOrchestrations; iteration++)
            {
                int globalIteration = iteration + input.offset;
                var orchestrationInstanceId = ManyOrchestrations.InstanceId(globalIteration);
                Entity.Current.StartNewOrchestration(input.orchestrationName, input.input ?? globalIteration.ToString(), orchestrationInstanceId);
            };
        }

        [FunctionName(nameof(LauncherEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx)
            => ctx.DispatchAsync<LauncherEntity>();
    }
}
