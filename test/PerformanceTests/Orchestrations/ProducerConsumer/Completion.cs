// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.ProducerConsumer
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

    public static class Completion
    {
        public enum Ops { Init, ProducerDone, ConsumerDone, Get, Ping }

        public class State
        {
            public int completedProducers;
            public int completedConsumers;
            public DateTime? productionCompleted;
            public DateTime? consumptionCompleted;
            public Parameters parameters;
        }

        [FunctionName(nameof(Completion))]
        public static Task HandleOperation(
            [EntityTrigger] IDurableEntityContext context, ILogger log)
        {
            State state = context.GetState(() => new State());

            switch (Enum.Parse<Ops>(context.OperationName))
            {
                case Ops.Init:
                    state.completedConsumers = 0;
                    state.parameters = context.GetInput<Parameters>();
                    log.LogWarning($"Initialized at {DateTime.UtcNow:o}");
                    break;
                case Ops.ProducerDone:
                    if (++state.completedProducers >= state.parameters.producers)
                    {
                        state.productionCompleted = DateTime.UtcNow;
                        log.LogWarning($"Completed Production at {DateTime.UtcNow:o}");
                    }
                    break;
                case Ops.ConsumerDone:
                    if (++state.completedConsumers >= state.parameters.consumers)
                    {
                        state.consumptionCompleted = DateTime.UtcNow;
                        log.LogWarning($"Completed Consumption at {DateTime.UtcNow:o}");
                    }
                    break;
                case Ops.Get:
                    context.Return(state);
                    break;
                case Ops.Ping:
                    break;
            }

            return Task.CompletedTask;
        }
    }
}