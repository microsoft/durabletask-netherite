// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.EventHubs
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Producer;
    using Microsoft.Azure.Documents.SystemFunctions;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    [JsonObject(MemberSerialization.OptIn)]
    public class ProducerEntity
    {
        public static EntityId GetEntityId(int number)
        {
            return new EntityId(nameof(ProducerEntity), $"!{number}");
        }

        [JsonProperty]
        public bool IsActive { get; set; }

        [JsonProperty]
        public long SentEvents { get; set; }

        [JsonProperty]
        public int Exceptions { get; set; }

        readonly ILogger logger;

        public ProducerEntity(ILogger logger)
        {
            this.logger = logger;
        }

        public Task Start()
        {
            this.IsActive = true;
            return this.ProduceMore();
        }

        public void Stop()
        {
            this.IsActive = false;
        }

        public void Delete()
        {
            Entity.Current.DeleteState();
        }

        public string CreateRandomPayload(Random random, int length)
        {
            if (length == 0)
            {
                return string.Empty;
            }
            var sb = new StringBuilder();
            for (int i = 0; i < length; i++)
            {
                sb.Append(Convert.ToChar(random.Next(0, 26) + 65));
            }
            return sb.ToString();
        }

        async Task ProduceMore()
        {
            if (!this.IsActive)
            {
                return;
            }

            var sw = new Stopwatch();
            sw.Start();

            int myNumber = int.Parse(Entity.Current.EntityId.EntityKey.Substring(1));

            Random random = new Random(myNumber);

            await using (var producer = new EventHubProducerClient(Parameters.EventHubConnectionString, Parameters.EventHubNameForProducer(myNumber)))
            {
                var r = new Random();

                while (sw.Elapsed < TimeSpan.FromSeconds(5))
                {
                    try
                    {
                        using EventDataBatch eventBatch = await producer.CreateBatchAsync();
                        using MemoryStream stream = new MemoryStream();
                        int batchsize = 100;
                        for (int i = 0; i < batchsize; i++)
                        {
                            var evt = new Event()
                            {
                                Destination = r.Next(Parameters.NumberDestinationEntities),
                                Payload = this.CreateRandomPayload(random, Parameters.PayloadStringLength),
                            };
                            eventBatch.TryAdd(new EventData(evt.ToBytes()));
                        }

                        this.SentEvents += eventBatch.Count;
                        await producer.SendAsync(eventBatch);
                    }

                    catch (Exception e)
                    {
                        this.Exceptions++;
                        this.logger.LogError("{entityId} Failed to send events: {exception}", Entity.Current.EntityId, e);
                    }
                }

                // schedule a continuation to send more
                Entity.Current.SignalEntity(Entity.Current.EntityId, nameof(ProduceMore));
            }
        }

        [FunctionName(nameof(ProducerEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext context, ILogger logger)
        {
            return context.DispatchAsync<ProducerEntity>(logger);   
        }
    }
}
 