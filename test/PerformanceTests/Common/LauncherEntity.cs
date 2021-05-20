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
        public void Launch((string orchestrationName, int numberOrchestrations, int offset) input)
        {
            // start all the orchestrations
            for (int iteration = 0; iteration < input.numberOrchestrations; iteration++)
            {
                var orchestrationInstanceId = ManyOrchestrations.InstanceId(iteration + input.offset);
                Entity.Current.StartNewOrchestration(input.orchestrationName, null, orchestrationInstanceId);
            };
        }

        [FunctionName(nameof(LauncherEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx)
            => ctx.DispatchAsync<LauncherEntity>();
    }
}
