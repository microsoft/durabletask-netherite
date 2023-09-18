// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.EventHubs
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public static class DestinationOrchestration
    {
        [FunctionName(nameof(DestinationOrchestration))]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger logger)
        {
            var input = context.GetInput<(Event evt, int receiverPartition)> ();

            logger.LogInformation("Started orchestration {instanceId}", context.InstanceId);

            // execute an activity with the event payload as the input
            await context.CallActivityAsync<string>(nameof(Activities.SayHello), input.evt.Payload);   

            logger.LogInformation("Completed orchestration {instanceId}", context.InstanceId);

            // when this orchestration completes, we notify the puller entity
            context.SignalEntity(PullerEntity.GetEntityId(input.receiverPartition), nameof(PullerEntity.Ack));
        }
    }
}
