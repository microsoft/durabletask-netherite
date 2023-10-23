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

    [JsonObject(MemberSerialization.OptIn)]
    public class DestinationEntity
    {
        public static EntityId GetEntityId(int number) => new EntityId(nameof(DestinationEntity), $"!{number}");

        [JsonProperty]
        public int EventCount { get; set; }

        [JsonProperty]
        public DateTime? LastUpdated { get; set; }


        ILogger logger;

        public DestinationEntity(ILogger logger)
        {
            this.logger = logger;
        }

        public ILogger Logger { set { this.logger = value; } }

        public void Delete()
        {
            Entity.Current.DeleteState();
        }

        public void Receive((Event evt, int receiverPartition) input)
        {
            this.EventCount++;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                this.logger.LogInformation("{entityId} Received signal #{eventCount}", Entity.Current.EntityId, this.EventCount);
            }

            this.LastUpdated = DateTime.UtcNow;

            // send an ack to the receiver entity
            Entity.Current.SignalEntity(PullerEntity.GetEntityId(input.receiverPartition), nameof(PullerEntity.Ack));
        }

        [FunctionName(nameof(DestinationEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext context, ILogger logger)
        {
            return context.DispatchAsync<DestinationEntity>(logger);
        }
    }
}
