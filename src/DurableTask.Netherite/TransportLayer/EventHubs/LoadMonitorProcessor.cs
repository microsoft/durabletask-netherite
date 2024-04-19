// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Processor;
    using Azure.Storage.Blobs.Specialized;
    using DurableTask.Core.Common;
    using DurableTask.Netherite.Abstractions;
    using Microsoft.Extensions.Logging;

    class LoadMonitorProcessor : IEventProcessor
    {
        readonly TransportAbstraction.IHost host;
        readonly TransportAbstraction.ISender sender;
        readonly TaskhubParameters parameters;
        readonly EventHubsTraceHelper traceHelper;
        readonly EventHubsTransport eventHubsTransport;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly string eventHubName;
        readonly string eventHubPartition;
        readonly byte[] taskHubGuid;
        readonly uint partitionId;
        readonly CancellationToken shutdownToken;
        readonly BlobBatchReceiver<LoadMonitorEvent> blobBatchReceiver;
        readonly EventProcessorClient client;

        TransportAbstraction.ILoadMonitor loadMonitor;
        DateTime lastGarbageCheck = DateTime.MinValue;

        readonly static TimeSpan GarbageCheckFrequency = TimeSpan.FromMinutes(30);

        public LoadMonitorProcessor(
            TransportAbstraction.IHost host,
            TransportAbstraction.ISender sender,
            TaskhubParameters parameters,
            EventProcessorClient client,
            string partitionId,
            NetheriteOrchestrationServiceSettings settings,
            EventHubsTransport eventHubsTransport,
            EventHubsTraceHelper traceHelper,
            CancellationToken shutdownToken)
        {
            this.host = host;
            this.sender = sender;
            this.parameters = parameters;
            this.settings = settings;
            this.client = client;
            this.eventHubName = this.client.EventHubName;
            this.eventHubPartition = partitionId;
            this.taskHubGuid = parameters.TaskhubGuid.ToByteArray();
            this.partitionId = uint.Parse(this.eventHubPartition);
            this.traceHelper = new EventHubsTraceHelper(traceHelper, this.partitionId);
            this.eventHubsTransport = eventHubsTransport;
            this.shutdownToken = shutdownToken;
            this.blobBatchReceiver = new BlobBatchReceiver<LoadMonitorEvent>("LoadMonitor", traceHelper, settings);
        }

        Task<EventPosition> IEventProcessor.OpenAsync(CancellationToken cancellationToken)
        {
            this.traceHelper.LogInformation("LoadMonitor is opening", this.eventHubName, this.eventHubPartition);
            this.loadMonitor = this.host.AddLoadMonitor(this.parameters.TaskhubGuid, this.sender);
            this.traceHelper.LogInformation("LoadMonitor opened", this.eventHubName, this.eventHubPartition);
            this.PeriodicGarbageCheck();
            return Task.FromResult(EventPosition.FromEnqueuedTime(DateTime.UtcNow - TimeSpan.FromSeconds(30)));
        }

        async Task IEventProcessor.CloseAsync(ProcessingStoppedReason reason, CancellationToken cancellationToken)
        {
            this.traceHelper.LogInformation("LoadMonitor is closing", this.eventHubName, this.eventHubPartition);
            await this.loadMonitor.StopAsync();
            this.traceHelper.LogInformation("LoadMonitor closed", this.eventHubName, this.eventHubPartition);
        }

        async Task IEventProcessor.ProcessErrorAsync(Exception exception, CancellationToken cancellationToken)
        {
            if (exception is Azure.Messaging.EventHubs.EventHubsException eventHubsException)
            {
                switch (eventHubsException.Reason)
                {
                    case EventHubsException.FailureReason.ConsumerDisconnected:
                        this.traceHelper.LogInformation("LoadMonitor received ConsumerDisconnected notification");
                        return;

                    case EventHubsException.FailureReason.InvalidClientState:
                        // something is permantently broken inside EH client, let's try to recover via restart 
                        this.traceHelper.LogError("LoadMonitor received InvalidClientState notification, initiating recovery via restart");
                        await this.eventHubsTransport.ExitProcess(false);
                        return;
                }
            }
       
            this.traceHelper.LogWarning("LoadMonitor received internal error indication from EventProcessorHost: {exception}", exception);
        }

        async Task IEventProcessor.ProcessEventBatchAsync(IEnumerable<EventData> packets, CancellationToken cancellationToken)
        {         
            this.traceHelper.LogTrace("LoadMonitor receiving #{seqno}", packets.First().SequenceNumber);
            try
            {         
                EventData last = null;

                int totalEvents = 0;
                List<BlockBlobClient> blobBatches = null;
                var stopwatch = Stopwatch.StartNew();
             
                await foreach ((EventData eventData, LoadMonitorEvent[] events, long seqNo, BlockBlobClient blob) in this.blobBatchReceiver.ReceiveEventsAsync(this.taskHubGuid, packets, this.shutdownToken))
                {
                    for (int i = 0; i < events.Length; i++)
                    {
                        var loadMonitorEvent = events[i];      
                        this.traceHelper.LogTrace("LoadMonitor receiving packet #{seqno}.{subSeqNo} {event} id={eventId}", seqNo, i, loadMonitorEvent, loadMonitorEvent.EventIdString);
                        this.loadMonitor.Process(loadMonitorEvent);
                        totalEvents++;
                    }
                    
                    last = eventData;

                    if (blob != null)
                    {
                        (blobBatches ??= new()).Add(blob);
                    }
                }

                if (last != null)
                {
                    this.traceHelper.LogDebug("LoadMonitor received {totalEvents} events in {latencyMs:F2}ms, through #{seqno}", totalEvents, stopwatch.Elapsed.TotalMilliseconds, last.SequenceNumber);
                }
                else
                {
                    this.traceHelper.LogDebug("LoadMonitor received no new events in {latencyMs:F2}ms", stopwatch.Elapsed.TotalMilliseconds);
                }

                if (blobBatches != null)
                {
                    Task backgroundTask = Task.Run(() => this.blobBatchReceiver.DeleteBlobBatchesAsync(blobBatches));
                }

                this.PeriodicGarbageCheck();
            }
            catch (OperationCanceledException) when (this.shutdownToken.IsCancellationRequested) 
            {
                // normal during shutdown
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("LoadMonitor encountered an exception while processing packets : {exception}", exception);
                throw;
            }
            finally
            {
                this.traceHelper.LogDebug("LoadMonitor exits receive loop");
            }
        }

        void PeriodicGarbageCheck()
        {
            if (DateTime.UtcNow - this.lastGarbageCheck > GarbageCheckFrequency)
            {
                this.lastGarbageCheck = DateTime.UtcNow;

                Task.Run(async () =>
                {
                    var stopwatch = Stopwatch.StartNew();

                    int deletedCount = await this.blobBatchReceiver.RemoveGarbageAsync(this.shutdownToken);

                    this.traceHelper.LogInformation("LoadMonitor removed {deletedCount} expired blob batches in {elapsed:F2}s", deletedCount, stopwatch.Elapsed.TotalSeconds);
                });
            }
        }
    }
}
