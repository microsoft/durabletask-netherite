// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.EventHubs
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Newtonsoft.Json;

    [JsonObject(MemberSerialization.OptOut)]
    public static class Parameters
    {
        /// <summary>
        /// The name of the event hubs connection
        /// </summary>
        public const string EventHubsConnectionName = "EventHubsConnection";

        /// <summary>
        /// The name of the Event Hub that connects the consumer and producer
        /// </summary>
        public const string EventHubName = "eventstest";

        /// <summary>
        /// Routing function (determines target entity for an event) 
        /// </summary>
        public static EntityId GetDestinationEntity(Event evt, int numberEntities) => ReceiverEntity.GetEntityId(Math.Abs((evt.Partition, evt.SeqNo).GetHashCode()) % numberEntities);
    }
}