// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.ProducerConsumer
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

    public static class Consumer
    {
        public enum Ops { Init, Item, Ping }

        public class State
        {
            public int[] count;
            public string error;
            public Parameters parameters;
        }

        [FunctionName(nameof(Consumer))]
        public static Task HandleOperation(
            [EntityTrigger] IDurableEntityContext context, ILogger log)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var state = context.GetState(() => new State());

            log.LogTrace("{Entity} loaded state in {Latency:F3}ms", context.EntityId, stopwatch.Elapsed.TotalMilliseconds);

            switch (Enum.Parse<Ops>(context.OperationName))
            {
                case Ops.Init:
                    log.LogInformation("{Consumer} initializing; functionInstanceId {FunctionInstanceId}", context.EntityId, context.FunctionBindingContext.FunctionInstanceId);
                    state.parameters = context.GetInput<Parameters>();
                    state.count = new int[state.parameters.producers];
                    break;

                case Ops.Item:
                    var input = context.GetInput<string>();
                    log.LogTrace("{Entity} read input in {Latency:F3}ms", context.EntityId, stopwatch.Elapsed.TotalMilliseconds);

                    var (producer, index) = Producer.Decode(input);

                    if (state.count[producer] == index)
                    {
                        log.LogTrace("{Consumer} received item {Count} from {Producer}", context.EntityId, index, producer);
                    }
                    else
                    {
                        state.error ??= $"producer:{producer} expected:{state.count[producer]} actual:{index}";
                        log.LogError("{Consumer} received item {Count} from {Producer}, expected {Expected}", context.EntityId, index, producer, state.count[producer]);
                    }

                    state.count[producer]++;

                    if (state.count.Sum() == state.parameters.SignalsPerConsumer)
                    {
                        context.SignalEntity(state.parameters.GetCompletionEntity(), nameof(Completion.Ops.ConsumerDone));
                    }
                    break;

                case Ops.Ping:
                    break;

            }

            log.LogInformation("{Entity} performed {Operation} in {Latency:F3}ms", context.EntityId, context.OperationName, stopwatch.Elapsed.TotalMilliseconds);

            return Task.CompletedTask;
        }
    }
}