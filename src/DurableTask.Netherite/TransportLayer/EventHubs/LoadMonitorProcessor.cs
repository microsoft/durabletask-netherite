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
    using DurableTask.Core.Common;
    using DurableTask.Netherite.Abstractions;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Extensions.Logging;

    class LoadMonitorProcessor : IEventProcessor
    {
        readonly TransportAbstraction.IHost host;
        readonly TransportAbstraction.ISender sender;
        readonly TaskhubParameters parameters;
        readonly EventHubsTraceHelper traceHelper;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly PartitionContext partitionContext;
        readonly string eventHubName;
        readonly string eventHubPartition;
        readonly byte[] taskHubGuid;
        readonly uint partitionId;
        readonly CancellationToken shutdownToken;
        readonly BlobBatchReceiver<LoadMonitorEvent> blobBatchReceiver;

        TransportAbstraction.ILoadMonitor loadMonitor;
        DateTime lastGarbageCheck = DateTime.MinValue;

        readonly static TimeSpan GarbageCheckFrequency = TimeSpan.FromMinutes(30);

        public LoadMonitorProcessor(
            TransportAbstraction.IHost host,
            TransportAbstraction.ISender sender,
            TaskhubParameters parameters,
            PartitionContext partitionContext,
            NetheriteOrchestrationServiceSettings settings,
            EventHubsTraceHelper traceHelper,
            CancellationToken shutdownToken)
        {
            this.host = host;
            this.sender = sender;
            this.parameters = parameters;
            this.partitionContext = partitionContext;
            this.settings = settings;
            this.eventHubName = this.partitionContext.EventHubPath;
            this.eventHubPartition = this.partitionContext.PartitionId;
            this.taskHubGuid = parameters.TaskhubGuid.ToByteArray();
            this.partitionId = uint.Parse(this.eventHubPartition);
            this.traceHelper = new EventHubsTraceHelper(traceHelper, this.partitionId);
            this.shutdownToken = shutdownToken;
            this.blobBatchReceiver = new BlobBatchReceiver<LoadMonitorEvent>("LoadMonitor", traceHelper, settings, keepUntilConfirmed: false);
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            this.traceHelper.LogInformation("LoadMonitor is opening", this.eventHubName, this.eventHubPartition);
            this.loadMonitor = this.host.AddLoadMonitor(this.parameters.TaskhubGuid, this.sender);
            this.traceHelper.LogInformation("LoadMonitor opened", this.eventHubName, this.eventHubPartition);
            this.PeriodicGarbageCheck();
            return Task.CompletedTask;
        }

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            this.traceHelper.LogInformation("LoadMonitor is closing", this.eventHubName, this.eventHubPartition);
            await this.loadMonitor.StopAsync();
            this.traceHelper.LogInformation("LoadMonitor closed", this.eventHubName, this.eventHubPartition);
        }

        Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception exception)
        {
            LogLevel logLevel;

            switch (exception)
            {
                case ReceiverDisconnectedException:
                    // occurs when partitions are being rebalanced by EventProcessorHost
                    logLevel = LogLevel.Information;
                    break;

                default:
                    logLevel = LogLevel.Warning;
                    break;
            }

            this.traceHelper.Log(logLevel, "LoadMonitor received internal error indication from EventProcessorHost: {exception}", exception);
            return Task.CompletedTask;
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> packets)
        {         
            this.traceHelper.LogTrace("LoadMonitor receiving #{seqno}", packets.First().SystemProperties.SequenceNumber);
            try
            {         
                EventData last = null;

                int totalEvents = 0;
                var stopwatch = Stopwatch.StartNew();
             
                await foreach ((EventData eventData, LoadMonitorEvent[] events, long seqNo) in this.blobBatchReceiver.ReceiveEventsAsync(this.taskHubGuid, packets, this.shutdownToken))
                {
                    for (int i = 0; i < events.Length; i++)
                    {
                        var loadMonitorEvent = events[i];      
                        this.traceHelper.LogTrace("LoadMonitor receiving packet #{seqno}.{subSeqNo} {event} id={eventId}", seqNo, i, loadMonitorEvent, loadMonitorEvent.EventIdString);
                        this.loadMonitor.Process(loadMonitorEvent);
                        totalEvents++;
                    }
                    
                    last = eventData;
                }


                if (last != null)
                {
                    this.traceHelper.LogDebug("LoadMonitor received {totalEvents} events in {latencyMs:F2}ms, through #{seqno}", totalEvents, stopwatch.Elapsed.TotalMilliseconds, last.SystemProperties.SequenceNumber);
                }
                else
                {
                    this.traceHelper.LogDebug("LoadMonitor received no new events in {latencyMs:F2}ms", stopwatch.Elapsed.TotalMilliseconds);
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
                this.traceHelper.LogInformation("LoadMonitor exits receive loop");
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
