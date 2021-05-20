// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.ProducerConsumer
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public static class ProducerConsumerOrchestration
    {
        [FunctionName(nameof(ProducerConsumerOrchestration))]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger logger)
        {
            var parameters = context.GetInput<Parameters>();

            void progress(string msg) 
            {
                if (!context.IsReplaying)
                {
                    logger.LogWarning($"{context.InstanceId} {context.CurrentUtcDateTime:o} {msg}");
                }
            }    

            // initialize all entities
            progress("Initializing Entities");

            var tasks = new[] { context.CallEntityAsync(parameters.GetCompletionEntity(), nameof(Completion.Ops.Init), parameters) }
            .Concat(Enumerable.Range(0, parameters.producers)
                    .Select(i => context.CallEntityAsync(parameters.GetProducerEntity(i), nameof(Producer.Ops.Init), parameters)))
            .Concat(Enumerable.Range(0, parameters.consumers)
                    .Select(i => context.CallEntityAsync(parameters.GetConsumerEntity(i), nameof(Consumer.Ops.Init), parameters)))
            .ToArray();

            await Task.WhenAll(tasks);

            // start sender entities
            progress("Starting Producers");

            for (int i = 0; i < parameters.producers; i++)
            {
                context.SignalEntity(
                    parameters.GetProducerEntity(i),
                    nameof(Producer.Ops.Continue),
                    parameters);
            }

            DateTime startTime = context.CurrentUtcDateTime;

            // poll the completion entity until the expected count is reached
            string result = null;

            while ((context.CurrentUtcDateTime - startTime) < TimeSpan.FromMinutes(5))
            {
                progress("Checking for completion");

                var response = await context.CallEntityAsync<Completion.State>(parameters.GetCompletionEntity(), nameof(Completion.Ops.Get));

                if (response.productionCompleted.HasValue && response.consumptionCompleted.HasValue)
                {
                    result = JsonConvert.SerializeObject(new { 
                        startTime = startTime,
                        production = (response.productionCompleted.Value - startTime).TotalSeconds, 
                        consumption = (response.consumptionCompleted.Value - startTime).TotalSeconds,
                    }) + '\n';
                    break;
                }

                await context.CreateTimer(context.CurrentUtcDateTime + TimeSpan.FromSeconds(6), CancellationToken.None);
            }

            if (result == null)
            {
                result = $"Timed out after {(context.CurrentUtcDateTime - startTime)}\n";
            }

            else if (parameters.keepAliveMinutes > 0)
            {
                progress("Running keep-alive");

                var starttime = context.CurrentUtcDateTime;

                while (context.CurrentUtcDateTime < starttime + TimeSpan.FromMinutes(parameters.keepAliveMinutes))
                {
                    context.SignalEntity(parameters.GetCompletionEntity(), nameof(Completion.Ops.Ping));
                    for (int i = 0; i < parameters.producers; i++)
                    {
                        context.SignalEntity(parameters.GetProducerEntity(i), nameof(Producer.Ops.Ping));
                    }
                    for (int i = 0; i < parameters.consumers; i++)
                    {
                        context.SignalEntity(parameters.GetConsumerEntity(i), nameof(Consumer.Ops.Ping));
                    }

                    await context.CreateTimer<object>(context.CurrentUtcDateTime + TimeSpan.FromSeconds(15), null, CancellationToken.None);

                    progress($"Keepalive {parameters.testname}, elapsed = {(context.CurrentUtcDateTime - startTime).TotalMinutes}m");
                }
            }

            progress(result);

            return result;
        }
    }
}