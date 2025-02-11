// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Primitives;
    using Azure.Messaging.EventHubs.Processor;
    using Azure.Storage.Blobs;
    using DurableTask.Core.Common;
    using DurableTask.Netherite.Abstractions;
    //using Microsoft.Azure.Storage.Blob;
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
        readonly BlobContainerClient cloudBlobContainer;
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
            BlobContainerClient cloudBlobContainer,
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
            return Task.WhenAll(this.StopHostingPartitionsAsync(), this.StopHostingLoadMonitorAsync());
        }

        async Task StartHostingPartitionsAsync()
        {
            this.traceHelper.LogInformation($"EventHubsTransport is registering EventProcessorHost");

            string formattedCreationDate = this.connections.CreationTimestamp.ToString("o").Replace("/", "-");

            this.eventProcessorHost = this.settings.EventHubsConnection.CreateEventProcessorHost(new EventProcessorHost.ConstructorArguments()
            {
                EventHubName = EventHubsTransport.PartitionHub,
                ClientOptions = new Azure.Messaging.EventHubs.EventProcessorClientOptions() { PrefetchCount = 500 },
                ConsumerGroup = EventHubsTransport.PartitionConsumerGroup,
                BlobContainerClient = this.cloudBlobContainer,
                Factory = new PartitionEventProcessorFactory(this),
                TraceHelper = this.traceHelper,
            });

            await this.eventProcessorHost.StartProcessingAsync(this.shutdownToken).ConfigureAwait(false);

            this.traceHelper.LogInformation($"EventHubsTransport started EventProcessorHost");
        }

        async Task StartHostingLoadMonitorAsync()
        {
            this.traceHelper.LogInformation("EventHubsTransport is registering LoadMonitorHost");

            this.loadMonitorHost = this.settings.EventHubsConnection.CreateEventProcessorHost(new EventProcessorHost.ConstructorArguments()
            {
                EventHubName = EventHubsTransport.LoadMonitorHub,
                ClientOptions = new Azure.Messaging.EventHubs.EventProcessorClientOptions() { PrefetchCount = 500 },
                ConsumerGroup = EventHubsTransport.LoadMonitorConsumerGroup,
                BlobContainerClient = this.cloudBlobContainer,
                Factory = new LoadMonitorEventProcessorFactory(this),
                TraceHelper = this.traceHelper,
            });

            await this.loadMonitorHost.StartProcessingAsync(this.shutdownToken).ConfigureAwait(false);

            this.traceHelper.LogInformation($"EventHubsTransport started LoadMonitorHost");
        }
 
        async Task StopHostingPartitionsAsync()
        {
            this.traceHelper.LogInformation($"EventHubsTransport is stopping EventProcessorHost");

            await this.eventProcessorHost.StopProcessingAsync();

            this.traceHelper.LogInformation($"EventHubsTransport stopped EventProcessorHost");
        }

        async Task StopHostingLoadMonitorAsync()
        {
            this.traceHelper.LogInformation($"EventHubsTransport is stopping LoadMonitorHost");

            await this.loadMonitorHost.StopProcessingAsync();

            this.traceHelper.LogInformation($"EventHubsTransport stopped LoadMonitorHost");
        }

        public class PartitionEventProcessorFactory : IEventProcessorFactory
        {
            readonly EventHubsPartitionManager manager;

            public PartitionEventProcessorFactory(EventHubsPartitionManager transport)
            {
                this.manager = transport;
            }

            public IEventProcessor CreateEventProcessor(EventProcessorClient client, string partitionId)
            {
                return new PartitionProcessor(
                    this.manager.host,
                    this.manager.transport,
                    this.manager.parameters,
                    client,
                    partitionId,
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

            public IEventProcessor CreateEventProcessor(EventProcessorClient client, string partitionId)
            {
                return  new LoadMonitorProcessor(
                     this.manager.host,
                     this.manager.transport,
                     this.manager.parameters,
                     client,
                     partitionId,
                     this.manager.settings,
                     this.manager.transport,
                     this.manager.traceHelper,
                     this.manager.shutdownToken);
            }
        }    
    }
}
