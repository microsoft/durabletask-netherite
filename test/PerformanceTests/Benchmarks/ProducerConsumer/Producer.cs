// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.ProducerConsumer
{
    using System;
    using System.Diagnostics;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;

    public static class Producer
    {
        public enum Ops { Init, Continue, Ping }

        public class State
        {
            public int count;
            public Parameters parameters;
            public DateTime? startTime;
        }

        [FunctionName(nameof(Producer))]
        public static Task HandleOperation(
            [EntityTrigger] IDurableEntityContext context,
            ILogger log)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var state = context.GetState(() => new State());

            log.LogInformation("{Entity} loaded state in {Latency:F3}ms", context.EntityId, stopwatch.Elapsed.TotalMilliseconds);

            switch (Enum.Parse<Ops>(context.OperationName))
            {
                case Ops.Init:
                    state.count = 0;
                    state.parameters = context.GetInput<Parameters>();
                    break;

                case Ops.Continue:
                    {
                        var builder = new StringBuilder();
                        var producer = int.Parse(context.EntityKey.Substring(0, context.EntityKey.IndexOf('-')));

                        log.LogDebug("{Producer} is starting a batch", context.EntityId);

                        // send a batch of messages
                        for (int c = 0; c < state.parameters.batchsize && state.count < state.parameters.SignalsPerProducer; ++c, ++state.count)
                        {
                            var index = state.count / state.parameters.consumers;
                            var consumer = state.count % state.parameters.consumers;                   

                            context.SignalEntity(
                                state.parameters.GetConsumerEntity(consumer),
                                nameof(Consumer.Ops.Item),
                                Encode(builder, producer, index, state.parameters.messagesize));

                            log.LogTrace("{Producer} sent item ({Index},{Consumer})", context.EntityId, index, consumer);
                        }

                        if (state.count < state.parameters.SignalsPerProducer)
                        {
                            // send continue message to self
                            context.SignalEntity(context.EntityId, nameof(Ops.Continue));
                        }
                        else
                        {
                            // producer is finished
                            log.LogWarning("{Producer} is done", context.EntityId);
                            context.SignalEntity(state.parameters.GetCompletionEntity(), nameof(Completion.Ops.ProducerDone));
                        }
                    }
                    break;

                case Ops.Ping:
                    break;
            }

            log.LogInformation("{Entity} performed {Operation} in {Latency:F3}ms", context.EntityId, context.OperationName, stopwatch.Elapsed.TotalMilliseconds);

            return Task.CompletedTask;
        }
        
        static string Encode(StringBuilder builder, int sender, int index, int size)
        {
            builder.Clear();
            builder.Append(sender.ToString());
            builder.Append(',');
            builder.Append(index.ToString());
            builder.Append('.');
            // pad to get to desired size with UTF-32
            while (builder.Length * 4 < size)
            {
                builder.Append('a');
            }
            return builder.ToString();
        }

        public static (int sender, int index) Decode(string payload)
        {
            var pos1 = payload.IndexOf(',');
            var pos2 = payload.IndexOf('.');
            var sender = int.Parse(payload.Substring(0, pos1));
            var index = int.Parse(payload.Substring(pos1 + 1, pos2 - pos1 - 1));
            return (sender, index);
        }
    }
}
