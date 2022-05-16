// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.EventHubs
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Primitives;
    using Azure.Messaging.EventHubs.Producer;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    [JsonObject(MemberSerialization.OptIn)]
    public class PullerEntity
    {
        public static EntityId GetEntityId(int number)
        {
            return new EntityId(nameof(PullerEntity), $"!{number}");
        }

        readonly ILogger logger;

        public PullerEntity(ILogger logger)
        {
            this.logger = logger;      
        }

        // EventHub partition receivers are heavyweight; we use a cache so we can reuse them between entity invocations
        readonly static ConcurrentDictionary<int, PartitionReceiver> cache = new ConcurrentDictionary<int, PartitionReceiver>();


        [JsonProperty]
        public bool IsActive { get; set; }

        [JsonProperty]
        public long ReceivePosition { get; set; }

        [JsonProperty]
        public int NumPending { get; set; }

        [JsonProperty]
        public DateTime? FirstReceived { get; set; }


        public const int PendingLimit = 1000;

        [JsonProperty]
        public long TotalEventsPulled { get; set; }


        public async Task Start()
        {
            this.IsActive = true;   
            await this.PullMore();
        }

        public async Task Stop()
        {
            int number = int.Parse(Entity.Current.EntityId.EntityKey.Substring(1));

            try
            {
                if (cache.TryRemove(number, out var partitionReceiver))
                {
                    await partitionReceiver.DisposeAsync();
                }
            }
            catch(Exception e)
            {
                this.logger.LogWarning($"{Entity.Current.EntityKey} failed to dispose PartitionReceiver: {e}");
            }

            this.IsActive = false;
        }

        public async Task Delete()
        {
            await this.Stop();
            Entity.Current.DeleteState();
        }
        
        public Task Ack()
        {
            this.NumPending--;

             if (this.NumPending == 0)
                this.logger.LogDebug($"{Entity.Current.EntityKey} All Acks Received");

            return this.PullMore();
        }

        public async Task PullMore()
        {
            if (!this.IsActive)
            {
                return; // we are not currently active
            }

            if (Entity.Current.BatchPosition < Entity.Current.BatchSize - 1)
            {
                return; // we have more operations in this batch to process, so do those first
            }

            if (this.NumPending >= PendingLimit)
            {
                this.logger.LogDebug($"{Entity.Current.EntityKey} Reached Limit");
                return; // do not continue until we get more acks
            }
 
            try
            {
                int myNumber = int.Parse(Entity.Current.EntityId.EntityKey.Substring(1));

                PartitionReceiver receiver = cache.GetOrAdd(myNumber, (partitionId) =>
                {
                    this.logger.LogDebug($"{Entity.Current.EntityKey} Creating PartitionReceiver");
                    return new PartitionReceiver(
                        EventHubConsumerClient.DefaultConsumerGroupName,
                        Parameters.EventHubPartitionIdForPuller(myNumber),
                        EventPosition.FromSequenceNumber(this.ReceivePosition, false),
                        Parameters.EventHubConnectionString,
                        Parameters.EventHubNameForPuller(myNumber));
                });

                int batchSize = 1000;
                TimeSpan waitTime = TimeSpan.FromSeconds(10);

                this.logger.LogDebug($"{Entity.Current.EntityKey} Receiving events at {this.ReceivePosition}...");
                var eventBatch = await receiver.ReceiveBatchAsync(batchSize, waitTime);
                this.logger.LogDebug($"{Entity.Current.EntityKey} ...response received.");
                DateTime timestamp = DateTime.UtcNow;

                foreach (EventData eventData in eventBatch)
                {
                    try
                    {
                        var e = Event.FromStream(eventData.EventBody.ToStream());
                        this.logger.LogDebug($"Sending signal to destination {e.Destination}");
                        Entity.Current.SignalEntity(DestinationEntity.GetEntityId(e.Destination), nameof(DestinationEntity.Receive), (e, myNumber));
                        this.TotalEventsPulled++;
                        this.NumPending++;
                    }
                    catch (Exception e)
                    {
                        this.logger.LogError($"{Entity.Current.EntityKey} Failed to process event {eventData.SequenceNumber}: {e}");
                    }
                    this.ReceivePosition = eventData.SequenceNumber;
                }

                if (!this.FirstReceived.HasValue && this.TotalEventsPulled > 0)
                {
                    this.FirstReceived = timestamp;
                }

                // if we have not reached the limit of pending deliveries yet, continue
                if (this.NumPending < PendingLimit)
                {
                    this.logger.LogDebug($"{Entity.Current.EntityKey} Scheduling continuation");
                    Entity.Current.SignalEntity(Entity.Current.EntityId, nameof(PullMore));
                }
            }
            catch (Exception e)
            {
                this.logger.LogError($"{Entity.Current.EntityKey} failed: {e}");
                this.IsActive = false;
            }
        }

        [FunctionName(nameof(PullerEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext context, ILogger logger)
        {
            return context.DispatchAsync<PullerEntity>(logger);   
        }
    }
}
 