// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;

    interface IEventProcessorFactory
    {
        IEventProcessor CreateEventProcessor(EventProcessorClient eventProcessorClient, string partitionId);
    }
}
