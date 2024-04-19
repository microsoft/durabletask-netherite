// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Processor;

    interface IEventProcessor
    {
        Task<EventPosition> OpenAsync(CancellationToken cancellationToken);

        Task ProcessEventBatchAsync(IEnumerable<EventData> events, CancellationToken cancellationToken);

        Task CloseAsync(ProcessingStoppedReason reason, CancellationToken cancellationToken);

        Task ProcessErrorAsync(Exception error, CancellationToken cancellationToken);
    }
}
