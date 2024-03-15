// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using DurableTask.Core.Common;
    using DurableTask.Netherite.Abstractions;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// The partition manager natively provided by EH
    /// </summary>
    class EventHubsPartitionManager : IPartitionManager
    {
        readonly TransportAbstraction.IHost host;
        readonly EventHubsConnections connections;
        readonly TaskhubParameters parameters;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly EventHubsTraceHelper traceHelper;
        readonly CloudBlobContainer cloudBlobContainer;
        readonly string pathPrefix;
        readonly EventHubsTransport transport;
        readonly CancellationToken shutdownToken;

        EventProcessorHost eventProcessorHost;
        EventProcessorHost loadMonitorHost;

        public EventHubsPartitionManager(
            TransportAbstraction.IHost host,
            EventHubsConnections connections,
            TaskhubParameters parameters,
            NetheriteOrchestrationServiceSettings settings,
            EventHubsTraceHelper traceHelper,
            CloudBlobContainer cloudBlobContainer,
            string pathPrefix,
            EventHubsTransport transport,
            CancellationToken shutdownToken)
        {
            this.host = host;
            this.connections = connections;
            this.parameters = parameters;
            this.settings = settings;
            this.traceHelper = traceHelper;
            this.cloudBlobContainer = cloudBlobContainer;
            this.pathPrefix = pathPrefix;
            this.transport = transport;
            this.shutdownToken = shutdownToken;
        }

        public string Fingerprint => this.connections.Fingerprint;

        public Task StartHostingAsync()
        {
            return Task.WhenAll(this.StartHostingPartitionsAsync(), this.StartHostingLoadMonitorAsync());
        }

        public Task StopHostingAsync()
        {
            return Task.WhenAll(this.eventProcessorHost.UnregisterEventProcessorAsync(), this.loadMonitorHost.UnregisterEventProcessorAsync());
        }

        async Task StartHostingPartitionsAsync()
        {
            this.traceHelper.LogInformation($"EventHubsTransport is registering EventProcessorHost");

            string formattedCreationDate = this.connections.CreationTimestamp.ToString("o").Replace("/", "-");

            this.eventProcessorHost = await this.settings.EventHubsConnection.GetEventProcessorHostAsync(
                Guid.NewGuid().ToString(),
                EventHubsTransport.PartitionHub,
                EventHubsTransport.PartitionConsumerGroup,
                this.settings.BlobStorageConnection,
                this.cloudBlobContainer.Name,
                $"{this.pathPrefix}eh-checkpoints/{(EventHubsTransport.PartitionHub)}/{formattedCreationDate}");

            var processorOptions = new EventProcessorOptions()
            {
                MaxBatchSize = 300,
                PrefetchCount = 500,
            };

            await this.eventProcessorHost.RegisterEventProcessorFactoryAsync(
                new PartitionEventProcessorFactory(this),
                processorOptions);

            this.traceHelper.LogInformation($"EventHubsTransport started EventProcessorHost");
        }

        async Task StartHostingLoadMonitorAsync()
        {
            this.traceHelper.LogInformation("EventHubsTransport is registering LoadMonitorHost");

            this.loadMonitorHost = await this.settings.EventHubsConnection.GetEventProcessorHostAsync(
                    Guid.NewGuid().ToString(),
                    EventHubsTransport.LoadMonitorHub,
                    EventHubsTransport.LoadMonitorConsumerGroup,
                    this.settings.BlobStorageConnection,
                    this.cloudBlobContainer.Name,
                    $"{this.pathPrefix}eh-checkpoints/{EventHubsTransport.LoadMonitorHub}");

            var processorOptions = new EventProcessorOptions()
            {
                InitialOffsetProvider = (s) => EventPosition.FromEnqueuedTime(DateTime.UtcNow - TimeSpan.FromSeconds(30)),
                MaxBatchSize = 500,
                PrefetchCount = 500,
            };

            await this.loadMonitorHost.RegisterEventProcessorFactoryAsync(
                new LoadMonitorEventProcessorFactory(this),
                processorOptions);

            this.traceHelper.LogInformation($"EventHubsTransport started LoadMonitorHost");
        }

        class PartitionEventProcessorFactory : IEventProcessorFactory
        {
            readonly EventHubsPartitionManager manager;

            public PartitionEventProcessorFactory(EventHubsPartitionManager transport)
            {
                this.manager = transport;
            }

            public IEventProcessor CreateEventProcessor(PartitionContext context)
            {
                return new EventHubsProcessor(
                    this.manager.host,
                    this.manager.transport,
                    this.manager.parameters,
                    context,
                    this.manager.settings,
                    this.manager.transport,
                    this.manager.traceHelper,
                    this.manager.shutdownToken);
            }
        }

        class LoadMonitorEventProcessorFactory : IEventProcessorFactory
        {
            readonly EventHubsPartitionManager manager;

            public LoadMonitorEventProcessorFactory(EventHubsPartitionManager transport)
            {
                this.manager = transport;
            }

            public IEventProcessor CreateEventProcessor(PartitionContext context)
            {
                return new LoadMonitorProcessor(
                    this.manager.host,
                    this.manager.transport,
                    this.manager.parameters,
                    context,
                    this.manager.settings,
                    this.manager.traceHelper,
                    this.manager.shutdownToken);
            }
        }    
    }
}
