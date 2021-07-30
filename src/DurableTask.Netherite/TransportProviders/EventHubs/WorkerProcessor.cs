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

    class WorkerProcessor : BatchWorker<WorkerProcessor.Batch>, IEventProcessor
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

        Batch batch;

        public WorkerProcessor(
            TransportAbstraction.IHost host,
            TransportAbstraction.IWorker worker,
            TransportAbstraction.ISender sender,
            TaskhubParameters parameters,
            PartitionContext partitionContext,
            NetheriteOrchestrationServiceSettings settings,
            EventHubsTraceHelper traceHelper,
            CancellationToken shutdownToken)
            : base(nameof(WorkerProcessor), false, int.MaxValue, partitionContext.CancellationToken, null)
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
            this.batch = new Batch(this, 0);
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

            this.traceHelper.Log(logLevel, "EventHubsProcessor {eventHubName}/{eventHubPartition} received internal error indication from EventProcessorHost: {exception}", this.eventHubName, this.eventHubPartition, exception);
            return Task.CompletedTask;
        }

        public class Batch : TransportAbstraction.IDurabilityListener
        {
            readonly WorkerProcessor workerProcessor;

            public Batch(WorkerProcessor workerProcessor, long seqno)
            {
                this.workerProcessor = workerProcessor;
                this.NumberAcks = seqno == 0 ? 1 : 0;
                this.Id = seqno;
            }

            // initial value is high so completion check does not prematurely fire
            public volatile int NumberEvents = int.MaxValue;

            // counts acks, one per event, one for the previous, and one for NumberEvents being true
            public int NumberAcks;

            public Batch Next;

            public long Id;

            public Checkpoint Checkpoint;

            public void ConfirmDurable(Event evt)
            {
                this.Ack(evt.EventId.ToString());
            }

            public void Ack(string src)
            {
                var numAcks = Interlocked.Increment(ref this.NumberAcks);
                this.workerProcessor.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition} received ack {numAcks}/{outOf} from {src} for batch B{id}", this.workerProcessor.eventHubName, this.workerProcessor.eventHubPartition, numAcks, this.NumberEvents + 2, src, this.Id); ;

                if (numAcks == this.NumberEvents + 2)
                {
                    this.workerProcessor.Submit(this);
                    this.Next.Ack($"B{this.Id}");
                }
            }
        }

        protected override async Task Process(IList<Batch> batches)
        {
            if (batches.Count > 0)
            {
                Batch batch = batches[batches.Count - 1];
                Checkpoint checkpoint = batch.Checkpoint;

                this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition} initiating checkpoint for batch B{id}", this.eventHubName, this.eventHubPartition, batch.Id); ;

                int retries = 0;

                while (!this.partitionContext.CancellationToken.IsCancellationRequested)
                {
                    this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is checkpointing receive position through #{seqno}", this.eventHubName, this.eventHubPartition, checkpoint.SequenceNumber);
                    try
                    {
                        await this.partitionContext.CheckpointAsync(batch.Checkpoint).ConfigureAwait(false);

                        this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} checkpointed receive position through #{seqno}", this.eventHubName, this.eventHubPartition, checkpoint.SequenceNumber);

                        retries = 0;

                        break;
                    }
                    catch (Microsoft.Azure.EventHubs.Processor.LeaseLostException)
                    {
                        this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} lease expired.");

                        break;
                    }
                    catch (Exception e) when (!Utils.IsFatal(e))
                    {
                        this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} failed to checkpoint receive position: {e}", this.eventHubName, this.eventHubPartition, e);

                        await Task.Delay(TimeSpan.FromSeconds(5));

                        retries++; // TODO surface errors
                    }
                }
            }
        }

        Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> packets)
        {
            this.batch.Next = new Batch(this, this.batch.Id + 1); // can receive acks, but cannot have all acks yet

            // submit the events for processing, which means the acks can come trickling in
            (int numberEvents, Checkpoint checkPoint) = this.SubmitWork(context, packets);

            if (checkPoint != null)
            {
                this.batch.Checkpoint = checkPoint;
                this.batch.NumberEvents = numberEvents;
                this.batch.Ack("numberEvents");

                this.batch = this.batch.Next;
            }

            return Task.CompletedTask;
        }

        (int numberEvents, Checkpoint checkPoint) SubmitWork(PartitionContext context, IEnumerable<EventData> packets)
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
                    WorkerEvent workerEvent = null;
                    last = eventData;

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
                        this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition} received packet #{seqno} ({size} bytes) {event}", this.eventHubName, this.eventHubPartition, seqno, eventData.Body.Count, workerEvent);
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

                    this.worker.Process(workerEvent, this.batch);
                    count++;
                }

                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received batch B{id} of {batchsize} packets, through #{seqno}", this.eventHubName, this.eventHubPartition, this.batch.Id, count, last.SystemProperties.SequenceNumber);
                return (count, new Checkpoint(context.PartitionId, last.SystemProperties.Offset, last.SystemProperties.SequenceNumber));
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
    }
}
