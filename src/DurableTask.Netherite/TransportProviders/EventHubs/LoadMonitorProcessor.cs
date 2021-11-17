// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubs
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

        TransportAbstraction.ILoadMonitor loadMonitor;

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
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is opening", this.eventHubName, this.eventHubPartition);
            this.loadMonitor = this.host.AddLoadMonitor(this.parameters.TaskhubGuid, this.sender);
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} opened", this.eventHubName, this.eventHubPartition);
            return Task.CompletedTask;
        }

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is closing", this.eventHubName, this.eventHubPartition);
            await this.loadMonitor.StopAsync();
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} closed", this.eventHubName, this.eventHubPartition);
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

            this.traceHelper.Log(logLevel, "EventHubsProcessor {eventHubName}/{eventHubPartition} received internal error indication from EventProcessorHost: {exception}", this.eventHubName, this.eventHubPartition, exception);
            return Task.CompletedTask;
        }

        Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> packets)
        {         
            this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition} receiving #{seqno}", this.eventHubName, this.eventHubPartition, packets.First().SystemProperties.SequenceNumber);
            try
            {         
                EventData last = null;
                int count = 0;
                int ignored = 0;

                foreach (var eventData in packets)
                {
                    var seqno = eventData.SystemProperties.SequenceNumber;
                    LoadMonitorEvent loadMonitorEvent = null;
                    last = eventData;

                    try
                    {
                        Packet.Deserialize(eventData.Body, out loadMonitorEvent, this.taskHubGuid);
                    }
                    catch (Exception)
                    {
                        this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} could not deserialize packet #{seqno} ({size} bytes)", this.eventHubName, this.eventHubPartition, seqno, eventData.Body.Count);
                        throw;
                    }

                    if (loadMonitorEvent != null)
                    {
                        this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition} received packet #{seqno} ({size} bytes) {event}", this.eventHubName, this.eventHubPartition, seqno, eventData.Body.Count, loadMonitorEvent);
                        count++;
                        this.loadMonitor.Process(loadMonitorEvent);
                    }
                    else
                    {
                        this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} ignored packet #{seqno} for different taskhub", this.eventHubName, this.eventHubPartition, seqno);
                        ignored++;
                        continue;
                    }

                    if (context.CancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                }

                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received batch of {batchsize} packets, through #{seqno}", this.eventHubName, this.eventHubPartition, count, last.SystemProperties.SequenceNumber);
            }
            catch (OperationCanceledException)
            {
                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} was terminated", this.eventHubName, this.eventHubPartition);
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} encountered an exception while processing packets : {exception}", this.eventHubName, this.eventHubPartition, exception);
                throw;
            }

            return Task.CompletedTask;
        }
    }
}
