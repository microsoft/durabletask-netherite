// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using System;
    using System.Collections.Generic;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Primitives;
    using Azure.Messaging.EventHubs.Processor;
    using Azure.Storage.Blobs;
    using Microsoft.Extensions.Logging;

    class EventProcessorHost : EventProcessorClient
    {
        readonly IEventProcessor[] eventProcessors;
        readonly IEventProcessorFactory factory;
        readonly EventHubsTraceHelper traceHelper;
        readonly CancellationToken shutdownToken;
        const int maxPartitions = 32;

        public EventProcessorHost(ConstructorArguments args, string connectionString)
            : base(args.BlobContainerClient, args.ConsumerGroup, connectionString, args.EventHubName, args.ClientOptions)
        {
            this.eventProcessors = new IEventProcessor[maxPartitions];
            this.factory = args.Factory;
            this.traceHelper = args.TraceHelper;
            this.PartitionInitializingAsync += this.OpenPartitionAsync;
            this.ProcessEventAsync += this.ProcessPartitionEventAsync;
            this.PartitionClosingAsync += this.ClosePartitionAsync;
            this.ProcessErrorAsync += this.ErrorAsync;
        }

        public EventProcessorHost(ConstructorArguments args, string fullyQualifiedNamespace, Azure.Core.TokenCredential tokenCredential)
          : base(args.BlobContainerClient, args.ConsumerGroup, fullyQualifiedNamespace, args.EventHubName, tokenCredential, args.ClientOptions)
        {
            this.eventProcessors = new IEventProcessor[maxPartitions];
            this.factory = args.Factory;
            this.traceHelper = args.TraceHelper;
            this.PartitionInitializingAsync += this.OpenPartitionAsync;
            this.PartitionClosingAsync += this.ClosePartitionAsync;
            this.ProcessErrorAsync += this.ErrorAsync;
        }

        public struct ConstructorArguments
        {
            public string EventHubName;
            public EventHubsTraceHelper TraceHelper;
            public string ConsumerGroup;
            public EventProcessorClientOptions ClientOptions;
            public BlobContainerClient BlobContainerClient;
            public IEventProcessorFactory Factory;
        };

        async Task OpenPartitionAsync(PartitionInitializingEventArgs args)
        {
            if (this.shutdownToken.IsCancellationRequested)
            {
                return;
            }
      
            int partitionId = int.Parse(args.PartitionId);
            IEventProcessor processor = this.eventProcessors[partitionId];
             
            if (processor == null)
            {
                this.eventProcessors[partitionId] = processor = this.factory.CreateEventProcessor(this, args.PartitionId);
                EventPosition startPosition = await processor.OpenAsync(args.CancellationToken).ConfigureAwait(false);
                args.DefaultStartingPosition = startPosition;
            }
            else
            {
                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} received open for already active partition", this.EventHubName, partitionId);
                // no point in rethrowing - EH cannot deal with exceptions
            }
        }
             
        async Task ClosePartitionAsync(PartitionClosingEventArgs args)
        {
            int partitionId = int.Parse(args.PartitionId);
            IEventProcessor processor = this.eventProcessors[partitionId];
            this.eventProcessors[partitionId] = null;

            if (processor != null)
            {
                await processor.CloseAsync(args.Reason, args.CancellationToken).ConfigureAwait(false);       
            }
            else 
            {
                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} received close for inactive partition", this.EventHubName, partitionId);
                // no point in rethrowing - cannot deal with exceptions
            }
        }

        async Task ErrorAsync(ProcessErrorEventArgs args)
        {     
            if (args.PartitionId != null)
            {
                int partitionId = int.Parse(args.PartitionId);
                IEventProcessor processor = this.eventProcessors[partitionId];

                if (processor != null)
                {
                    await processor.ProcessErrorAsync(args.Exception, args.CancellationToken).ConfigureAwait(false);
                }
                else
                {
                    this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} received event hubs error indication: {exception}", this.EventHubName, args.PartitionId, args.Exception);
                }
            }
            else
            {
                this.traceHelper.LogWarning("EventHubsProcessor {eventHubName} received event hubs error indication: {exception}", this.EventHubName, args.Exception);
            }
        }

        Task ProcessPartitionEventAsync(ProcessEventArgs args)
        {
            // this should never get called because we override OnProcessingEventBatchAsync, see below
            this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} received invalid call to overridden ProcessEventAsync", this.EventHubName, args.Partition.PartitionId);
            return Task.CompletedTask;
        }

        protected override async Task OnProcessingEventBatchAsync(IEnumerable<EventData> events, EventProcessorPartition partition, CancellationToken cancellationToken)
        {
            int partitionId = int.Parse(partition.PartitionId);
            IEventProcessor processor = this.eventProcessors[partitionId];

            if (processor != null)
            {
                await processor.ProcessEventBatchAsync(events, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} received events for inactive partition", this.EventHubName, partitionId);
                // no point in rethrowing - EH cannot deal with exceptions
            }
        }  
    }
}
