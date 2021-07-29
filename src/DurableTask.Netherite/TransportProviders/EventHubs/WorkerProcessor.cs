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

    class WorkerProcessor : IEventProcessor
    {
        readonly TransportAbstraction.IHost host;
        readonly TransportAbstraction.IWorker worker;
        readonly TransportAbstraction.ISender sender;
        readonly TaskhubParameters parameters;
        readonly EventHubsTraceHelper traceHelper;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly PartitionContext partitionContext;
        readonly string eventHubName;
        readonly string eventHubPartition;
        readonly byte[] taskHubGuid;
        readonly uint partitionId;


        public WorkerProcessor(
            TransportAbstraction.IHost host,
            TransportAbstraction.IWorker worker,
            TransportAbstraction.ISender sender,
            TaskhubParameters parameters,
            PartitionContext partitionContext,
            NetheriteOrchestrationServiceSettings settings,
            EventHubsTraceHelper traceHelper,
            CancellationToken shutdownToken)
        {
            this.host = host;
            this.worker = worker;
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
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} opened", this.eventHubName, this.eventHubPartition);
            return Task.CompletedTask;
        }
       
        Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is closing", this.eventHubName, this.eventHubPartition);
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} closed", this.eventHubName, this.eventHubPartition);
            return Task.CompletedTask;
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

            this.traceHelper.Log(LogLevel.Warning, "EventHubsProcessor {eventHubName}/{eventHubPartition} received internal error indication from EventProcessorHost: {exception}", this.eventHubName, this.eventHubPartition, exception);
            return Task.CompletedTask;
        }

        public class Batch : TransportAbstraction.IDurabilityListener
        {
            // initial value is high so completion check does not prematurely fire
            public volatile int NumberEvents  = int.MaxValue;

            // counts acks, one per event, one for the previous, and one for NumberEvents being true
            public int NumberAcks;

            //one per event in the batch, one for the previous batch, and one for the NumberEvents having been set to the correct value
            public bool HaveAllAcks => this.NumberAcks == this.NumberEvents + 2;

            public Checkpoint Checkpoint;

            public Batch Next;

            public void ConfirmDurable(Event evt)
            {
                this.AckAndCheckpoint();
            }

            public bool AckAndCheckpoint()
            {
                var numAcks = Interlocked.Increment(ref this.NumberAcks);

                if (this.HaveAllAcks)
                {
                    if (this.Next?.AckAndCheckpoint() != true)
                    {
                        //submit checkpoint

                    }

                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        private Batch batch;

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> packets)
        {
            this.batch.Next = new Batch(); // can receive acks, but cannot have all acks yet

            // submit the events for processing, which means the acks can come trickling in
            (int numberEvents, Checkpoint checkPoint) = this.SubmitWorkEvents(packets);

            if (numberEvents > 0)
            {
                this.batch.Checkpoint = checkPoint;
                this.batch.NumberEvents = numberEvents;
                this.batch.AckAndCheckpoint();
            }

            this.batch = this.batch.Next;
        }


        (int numberEvents, Checkpoint checkPoint) SubmitWorkEvents(PartitionContext context, IEnumerable<EventData> packets)
        {
            this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition} receiving #{seqno}", this.eventHubName, this.eventHubPartition, packets.First().SystemProperties.SequenceNumber);
            try
            {
                EventData last = default;
                int count = 0;

                foreach (var eventData in packets)
                {
                    var seqno = eventData.SystemProperties.SequenceNumber;
                    WorkerEvent workerEvent = null;

                    try
                    {
                        Packet.Deserialize(eventData.Body, out workerEvent, this.taskHubGuid);
                    }
                    catch (Exception)
                    {
                        this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} could not deserialize packet #{seqno} ({size} bytes)", this.eventHubName, this.eventHubPartition, seqno, eventData.Body.Count);
                        throw;
                    }

                    if (workerEvent != null)
                    {
                        this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition} received packet #{seqno} ({size} bytes) {event}", this.eventHubName, this.eventHubPartition, seqno, eventData.Body.Count, partitionEvent);
                    }
                    else
                    {
                        this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} ignored packet #{seqno} for different taskhub", this.eventHubName, this.eventHubPartition, seqno);
                        continue;
                    }

                    DurabilityListeners.Register(workerEvent, this.batch);
                    this.worker.Process(workerEvent);
                    last = eventData;
                    count++;
                }

                if (count > 0)
                {
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received batch of {batchsize} packets", this.eventHubName, this.eventHubPartition, count);
                    return (count, new Checkpoint(context.PartitionId, last.SystemProperties.Offset, last.SystemProperties.SequenceNumber));
                }
                else
                {
                    return (0, null);
                }
            }
            catch (OperationCanceledException)
            {
                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} was terminated", this.eventHubName, this.eventHubPartition);
                return (0, null);
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} encountered an exception while processing packets : {exception}", this.eventHubName, this.eventHubPartition, exception);
                throw;
            }
        }


        async ValueTask SaveEventHubsReceiverCheckpoint(PartitionContext context)
        {
            var checkpoint = this.pendingCheckpoint;

            context.CheckpointAsync()
            if (checkpoint != null)
            {
                this.pendingCheckpoint = null;
                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is checkpointing receive position through #{seqno}", this.eventHubName, this.eventHubPartition, checkpoint.SequenceNumber);
                try
                {
                    await context.CheckpointAsync(checkpoint).ConfigureAwait(false);
                }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    // updating EventHubs checkpoints has been known to fail occasionally due to leases shifting around; since it is optional anyway
                    // we don't want this exception to cause havoc
                    this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} failed to checkpoint receive position: {e}", this.eventHubName, this.eventHubPartition, e);
                }
            }
        }

    }
}
