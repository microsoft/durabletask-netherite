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
    public class ReceiverEntity
    {
        public static EntityId GetEntityId(int index) => new EntityId(nameof(ReceiverEntity), index.ToString());

        [JsonProperty]
        public int EventCount { get; set; }

        [JsonProperty]
        public DateTime StartTime { get; set; }

        [JsonProperty]
        public DateTime LastTime { get; set; }

        [JsonProperty]
        public long BytesReceived { get; set; }

        ILogger logger;

        public ReceiverEntity(ILogger logger)
        {
            this.logger = logger;
        }

        public ILogger Logger { set { this.logger = value; } }

        public void Receive(Event evt)
        {
            this.LastTime = DateTime.UtcNow;

            if (this.EventCount++ == 0)
            {
                this.StartTime = this.LastTime;
            }

            this.BytesReceived += evt.Payload.Length;
           
            if (this.logger.IsEnabled(LogLevel.Information))
            {
                this.logger.LogInformation($"{Entity.Current.EntityId} Received event #{this.EventCount}: {evt}");
            }
        }

        [FunctionName(nameof(ReceiverEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext context, ILogger logger)
        {
            return context.DispatchAsync<ReceiverEntity>(logger);
        }
    }
}
 