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
    public class PartitionReceiverEntity
    {
        public static EntityId GetEntityId(string EventHubName, string partitionId)
        {
            return new EntityId(nameof(PartitionReceiverEntity), $"{EventHubName}!{int.Parse(partitionId):D2}");
        }

        [JsonProperty]
        int NumEntities { get; set; }

        [JsonProperty]
        string PartitionId { get; set; }

        [JsonProperty]
        long Position { get; set; }

        [JsonProperty]
        double BackoffSeconds { get; set; }

        [JsonProperty]
        TimeSpan? IdleInterval { get; set; }

        [JsonProperty]
        Guid InstanceGuid { get; set; }

        ILogger logger;

        readonly static ConcurrentDictionary<Guid, PartitionReceiver> cache = new ConcurrentDictionary<Guid, PartitionReceiver>();

        public PartitionReceiverEntity(ILogger logger)
        {
            this.logger = logger;
        }

        public ILogger Logger { set { this.logger = value; } }

        [JsonObject(MemberSerialization.OptOut)]
        public struct StartParameters
        {
            public int NumEntities { get; set; }

            public string PartitionId { get; set; }

            public long NextSequenceNumberToReceive { get; set; }

            public TimeSpan? IdleInterval { get; set; }
        }

        void ProcessEvent(EventData eventData)
        {
            byte[] payload = eventData.EventBody.ToArray();
            var evt = new Event()
            {
                Partition = payload[0],
                SeqNo = eventData.SequenceNumber,
                Payload = payload
            };
            this.logger.LogDebug($"Sending signal for {evt}");
            Entity.Current.SignalEntity(Parameters.GetDestinationEntity(evt, this.NumEntities), nameof(ReceiverEntity.Receive), evt);
        }

        public async Task Start(StartParameters parameters)
        {
            // close any receivers that may already exist because of a previous run
            if (cache.TryRemove(this.InstanceGuid, out var receiver))
            {
                await receiver.CloseAsync();
            }

            this.NumEntities = parameters.NumEntities;
            this.PartitionId = parameters.PartitionId;
            this.Position = parameters.NextSequenceNumberToReceive - 1;
            this.IdleInterval = parameters.IdleInterval;
            this.InstanceGuid = Guid.NewGuid();

            await this.Continue(this.InstanceGuid);
        }

        public async Task Delete()
        {
            if (cache.TryRemove(this.InstanceGuid, out var receiver))
            {
                await receiver.CloseAsync();
            }
            Entity.Current.DeleteState();
        }

        public async Task Continue(Guid instanceGuid)
        {
            try
            {
                if (this.InstanceGuid != instanceGuid)
                {
                    // the signal is from an instance that was deleted or overwritten. Ignore it.
                    return;
                }

                this.logger.LogInformation($"{Entity.Current.EntityKey} Continuing after {this.Position}");

                PartitionReceiver receiver = cache.GetOrAdd(this.InstanceGuid, (partitionId) =>
                {
                    this.logger.LogDebug($"{Entity.Current.EntityKey} Creating PartitionReceiver");

                    return new PartitionReceiver(
                    EventHubConsumerClient.DefaultConsumerGroupName,
                    this.PartitionId,
                    EventPosition.FromSequenceNumber(this.Position, false),
                    Environment.GetEnvironmentVariable("EventHubsConnection"),
                    Parameters.EventHubName);
                });

                int batchSize = 100;
                TimeSpan waitTime = TimeSpan.FromSeconds(1);
                long? lastReceivedSequenceNumber = null;

                this.logger.LogDebug($"{Entity.Current.EntityKey} Receiving events after {this.Position}...");
                var eventBatch = await receiver.ReceiveBatchAsync(batchSize, waitTime);
                this.logger.LogDebug($"{Entity.Current.EntityKey} ...response received.");

                foreach (EventData eventData in eventBatch)
                {
                    try
                    {
                        this.ProcessEvent(eventData);
                    }
                    catch (Exception e)
                    {
                        this.logger.LogError($"{Entity.Current.EntityKey} Failed to process event {eventData.SequenceNumber}: {e}");
                    }
                    lastReceivedSequenceNumber = eventData.SequenceNumber;
                }


                if (lastReceivedSequenceNumber.HasValue)
                {
                    this.logger.LogInformation($"Processed {lastReceivedSequenceNumber.Value - this.Position} signals");
                    this.Position = lastReceivedSequenceNumber.Value;
                    this.ScheduleContinuation(0);
                }
                else if (this.BackoffSeconds == 0)
                {
                    this.ScheduleContinuation(1);
                }
                else if (this.BackoffSeconds < 10)
                {
                    this.ScheduleContinuation(this.BackoffSeconds * 3);
                }
                else
                {
                    cache.TryRemove(this.InstanceGuid, out _);
                    await receiver.CloseAsync();

                    if (this.IdleInterval.HasValue)
                    {
                        this.logger.LogInformation($"{Entity.Current.EntityKey} Going on standby for {this.IdleInterval.Value}");
                        this.ScheduleContinuation(this.IdleInterval.Value.TotalSeconds);
                    }
                    else
                    {
                        this.logger.LogInformation($"{Entity.Current.EntityKey} is done");
                        Entity.Current.DeleteState();
                        return;
                    }
                } 
            }
            catch (Exception e) // TODO catch
            {
                this.logger.LogError($"{Entity.Current.EntityKey} failed: {e}");
                if (this.IdleInterval.HasValue)
                {
                    this.ScheduleContinuation(this.IdleInterval.Value.TotalSeconds);
                }
            }
        }

        void ScheduleContinuation(double BackoffSeconds)
        {
            if (BackoffSeconds == 0)
            {
                this.logger.LogDebug($"{Entity.Current.EntityKey} Scheduling continuation immediately");
                Entity.Current.SignalEntity(
                    Entity.Current.EntityId,
                    nameof(Continue),
                    this.InstanceGuid);
            }
            else
            {
                var targetTime = DateTime.UtcNow + TimeSpan.FromSeconds(BackoffSeconds);
                this.logger.LogDebug($"{Entity.Current.EntityKey} Scheduling continuation delayed by {BackoffSeconds}s");
                Entity.Current.SignalEntity(
                    Entity.Current.EntityId,
                    targetTime,
                    nameof(Continue),
                    this.InstanceGuid);
            }
            this.BackoffSeconds = BackoffSeconds;
        }

        [FunctionName(nameof(PartitionReceiverEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext context, ILogger logger)
        {
            return context.DispatchAsync<PartitionReceiverEntity>(logger);   
        }
    }
}
 