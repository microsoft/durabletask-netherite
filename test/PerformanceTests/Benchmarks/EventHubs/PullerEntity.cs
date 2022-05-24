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

        [JsonProperty]
        public int Errors { get; set; }

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
                this.logger.LogWarning("{entityId} failed to dispose PartitionReceiver: {exception}", Entity.Current.EntityId, e);
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

            if (this.NumPending >= Parameters.PendingLimit)
            {
                return; // do not continue until we get more acks
            }
 
            try
            {
                int myNumber = int.Parse(Entity.Current.EntityId.EntityKey.Substring(1));
                string myEntityId = Entity.Current.EntityId.ToString();

                PartitionReceiver receiver = cache.GetOrAdd(myNumber, (partitionId) =>
                {
                    this.logger.LogDebug("{entityId} Creating PartitionReceiver at position {receivePosition}", myEntityId, this.ReceivePosition);
                    return new PartitionReceiver(
                        EventHubConsumerClient.DefaultConsumerGroupName,
                        Parameters.EventHubPartitionIdForPuller(myNumber),
                        EventPosition.FromSequenceNumber(this.ReceivePosition - 1, false),
                        Parameters.EventHubConnectionString,
                        Parameters.EventHubNameForPuller(myNumber));
                });

                int batchSize = 1000;
                TimeSpan waitTime = TimeSpan.FromSeconds(10);

                this.logger.LogDebug("{entityId} Receiving events from position {receivePosition}", myEntityId, this.ReceivePosition);
                var eventBatch = await receiver.ReceiveBatchAsync(batchSize, waitTime);
                DateTime timestamp = DateTime.UtcNow;
                int numEventsInBatch = 0;

                foreach (EventData eventData in eventBatch)
                {
                    numEventsInBatch++;

                    if (eventData.SequenceNumber != this.ReceivePosition)
                    {
                        // for sanity, check that we received the next event in the sequence. 
                        // this can fail if events were discarded internally by the EventHub.
                        this.logger.LogError("{entityId} Received wrong event, sequence number expected={expected} actual={actual}", myEntityId, this.ReceivePosition, eventData.SequenceNumber);
                        this.Errors++;
                    }
                    
                    if (Parameters.SendEntitySignalForEachEvent || Parameters.StartOrchestrationForEachEvent)
                    {
                        try
                        {
                            // parse the event data
                            var e = Event.FromStream(eventData.EventBody.ToStream());

                            if (Parameters.SendEntitySignalForEachEvent)
                            {
                                EntityId destinationEntityId = DestinationEntity.GetEntityId(e.Destination);
                                this.logger.LogDebug("{entityId} Sending signal to {destinationEntityId}", myEntityId, destinationEntityId);
                                Entity.Current.SignalEntity(destinationEntityId, nameof(DestinationEntity.Receive), (e, myNumber));
                                this.NumPending++;
                            }

                            if (Parameters.StartOrchestrationForEachEvent)
                            {
                                var instanceId = this.GetRandomOrchestrationInstanceId(myNumber);
                                this.logger.LogDebug("{entityId} Scheduling orchestration {instanceId}", myEntityId, instanceId);
                                Entity.Current.StartNewOrchestration(nameof(DestinationOrchestration), (e, myNumber), instanceId);
                                this.NumPending++;
                            }
                        }
                        catch (Exception e)
                        {
                            this.logger.LogError("{entityId} Failed to process event {sequenceNumber}: {e}", myEntityId, eventData.SequenceNumber);
                        }
                    }

                    this.TotalEventsPulled++;
                    this.ReceivePosition = eventData.SequenceNumber + 1;
                }

                this.logger.LogDebug("{entityId} Processed batch of {numEventsInBatch} events", myEntityId, numEventsInBatch);

                if (!this.FirstReceived.HasValue && this.TotalEventsPulled > 0)
                {
                    this.FirstReceived = timestamp;
                }

                // if we have not reached the limit of pending deliveries yet, continue
                if (this.NumPending < Parameters.PendingLimit)
                {
                    this.logger.LogDebug("{entityId} Scheduling continuation for position {receivePosition}", myEntityId, this.ReceivePosition);
                    Entity.Current.SignalEntity(Entity.Current.EntityId, nameof(PullMore));
                }
            }
            catch (Exception e)
            {
                this.logger.LogError("{entityId} failed: {exception}", Entity.Current.EntityId, e);
                this.Errors++;
                this.IsActive = false;
            }
        }

        string GetRandomOrchestrationInstanceId(int myNumber)
        {
            if (Parameters.PlaceOrchestrationInstance)
            {
                return $"{Guid.NewGuid():n}!{myNumber}";
            }
            else
            {
                return $"{Guid.NewGuid():n}";
            }
        }

        [FunctionName(nameof(PullerEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext context, ILogger logger)
        {
            return context.DispatchAsync<PullerEntity>(logger);   
        }
    }
}
 