// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
/*
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs.Consumer;
    using DurableTask.Core.Common;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Logging;

    class EventHubsSender<T> : BatchWorker<Event> where T : Event
    {
        readonly PartitionSender sender;
        readonly TransportAbstraction.IHost host;
        readonly byte[] guid;
        readonly EventHubsTraceHelper traceHelper;
        readonly EventHubsTraceHelper lowestTraceLevel;
        readonly string eventHubName;
        readonly string eventHubPartition;
        readonly TimeSpan backoff = TimeSpan.FromSeconds(5);
        readonly MemoryStream stream = new MemoryStream(); // reused for all packets
        readonly Stopwatch stopwatch = new Stopwatch();
        readonly BlobBatchSender blobBatchSender;    

        public EventHubsSender(TransportAbstraction.IHost host, byte[] guid, PartitionSender sender, CancellationToken shutdownToken, EventHubsTraceHelper traceHelper, NetheriteOrchestrationServiceSettings settings)
           : base($"EventHubsSender {sender.EventHubClient.EventHubName}/{sender.PartitionId}", false, 2000, shutdownToken, traceHelper)
        {
            this.host = host;
            this.guid = guid;
            this.sender = sender;
            this.traceHelper = traceHelper;
            this.lowestTraceLevel = traceHelper.IsEnabled(LogLevel.Trace) ? traceHelper : null;
            this.eventHubName = this.sender.EventHubClient.EventHubName;
            this.eventHubPartition = this.sender.PartitionId;
            this.blobBatchSender = new BlobBatchSender($"EventHubsSender {this.eventHubName}/{this.eventHubPartition}", this.traceHelper, settings);
        }

        protected override async Task Process(IList<Event> toSend)
        {
            if (toSend.Count == 0)
            {
                return;
            }

            // track progress in case of exception
            var sentSuccessfully = -1;
            var maybeSent = -1;
            Exception senderException = null;

            // track current position in toSend
            int index = 0;

            // track offsets of the packets in the stream
            // since the first offset is always zero, there is one fewer than the number of packets
            List<int> packetOffsets = new List<int>(Math.Min(toSend.Count, this.blobBatchSender.MaxBlobBatchEvents) - 1);

            void CollectBatchContent(bool specificallyForBlob)
            {
                int maxEvents = specificallyForBlob ? this.blobBatchSender.MaxBlobBatchEvents : this.blobBatchSender.MaxEventHubsBatchEvents;
                int maxBytes = specificallyForBlob ? this.blobBatchSender.MaxBlobBatchBytes : this.blobBatchSender.MaxEventHubsBatchBytes;

                this.stream.Seek(0, SeekOrigin.Begin);
                packetOffsets.Clear();
                for (; index < toSend.Count && index < maxEvents && this.stream.Position < maxBytes; index++)
                {
                    int currentOffset = (int) this.stream.Position;
                    if (currentOffset > 0)
                    {
                        packetOffsets.Add(currentOffset);
                    }

                    var evt = toSend[index];
                    this.lowestTraceLevel?.LogTrace("EventHubsSender {eventHubName}/{eventHubPartitionId} is sending event {evt} id={eventId}", this.eventHubName, this.eventHubPartition, evt, evt.EventIdString);
                    if (!specificallyForBlob)
                    {
                        Packet.Serialize(evt, this.stream, this.guid);
                    }
                    else
                    {
                        // we don't need to include the task hub guid if the event is sent via blob
                        Packet.Serialize(evt, this.stream);
                    }                 
                }
            }

            try
            {
                // unless the total number of events is above the max already, we always check first if we can avoid using a blob
                bool usingBlobBatches = toSend.Count > this.blobBatchSender.MaxEventHubsBatchEvents; 

                while (index < toSend.Count)
                {
                    CollectBatchContent(usingBlobBatches);

                    if (!usingBlobBatches)
                    {
                        if (index == toSend.Count
                            && index <= this.blobBatchSender.MaxEventHubsBatchEvents
                            && this.stream.Position <= this.blobBatchSender.MaxEventHubsBatchBytes)
                        {
                            // we don't have a lot of bytes or messages to send
                            // send them all in a single EH batch
                            using var batch = this.sender.CreateBatch();
                            long maxPosition = this.stream.Position;
                            this.stream.Seek(0, SeekOrigin.Begin);
                            var buffer = this.stream.GetBuffer();
                            for (int j = 0; j < index; j++)
                            {
                                int offset = j == 0 ? 0 : packetOffsets[j - 1];
                                int nextOffset = j < packetOffsets.Count ? packetOffsets[j] : (int) maxPosition;
                                var length = nextOffset - offset;
                                var arraySegment = new ArraySegment<byte>(buffer, offset, length);
                                var eventData = new EventData(arraySegment);
                                if (batch.TryAdd(eventData))
                                {
                                    Event evt = toSend[j];
                                    this.lowestTraceLevel?.LogTrace("EventHubsSender {eventHubName}/{eventHubPartitionId} added packet to batch offset={offset} length={length} {evt} id={eventId}", this.eventHubName, this.eventHubPartition, offset, length, evt, evt.EventIdString);
                                }
                                else
                                {
                                    throw new InvalidOperationException("could not add event to batch"); // should never happen as max send size is very small
                                }
                            }
                            maybeSent = index - 1;
                            this.stopwatch.Restart();
                            await this.sender.SendAsync(batch).ConfigureAwait(false);
                            this.stopwatch.Stop();
                            sentSuccessfully = index - 1;
                            this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} sent batch of {numPackets} packets ({size} bytes) in {latencyMs:F2}ms, throughput={throughput:F2}MB/s", this.eventHubName, this.eventHubPartition, batch.Count, batch.Size, this.stopwatch.Elapsed.TotalMilliseconds, batch.Size / (1024 * 1024 * this.stopwatch.Elapsed.TotalSeconds));
                            break; // all messages were sent
                        }
                        else
                        {
                            usingBlobBatches = true;
                        }
                    }

                    // send the event(s) as a blob batch
                    this.stopwatch.Restart();
                    EventData blobMessage = await this.blobBatchSender.UploadEventsAsync(this.stream, packetOffsets, this.guid, this.cancellationToken);
                    maybeSent = index - 1;
                    await this.sender.SendAsync(blobMessage);
                    this.stopwatch.Stop();
                    sentSuccessfully = index - 1;
                    this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} sent blob-batch of {numPackets} packets ({size} bytes) in {latencyMs:F2}ms, throughput={throughput:F2}MB/s", this.eventHubName, this.eventHubPartition, packetOffsets.Count + 1, this.stream.Position, this.stopwatch.Elapsed.TotalMilliseconds, this.stream.Position / (1024 * 1024 * this.stopwatch.Elapsed.TotalSeconds));
                }
            }
            catch (OperationCanceledException) when (this.cancellationToken.IsCancellationRequested)
            {
                // normal during shutdown
                this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} was cancelled", this.eventHubName, this.eventHubPartition);
                return;
            }
            catch (OperationCanceledException) when (this.cancellationToken.IsCancellationRequested)
            {
                // normal during shutdown
                this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} was cancelled", this.eventHubName, this.eventHubPartition);
                return;
            }
            catch (Exception e)
            {
                this.traceHelper.LogWarning("EventHubsSender {eventHubName}/{eventHubPartitionId} failed to send: {e}", this.eventHubName, this.eventHubPartition, e);
                senderException = e;

                if (Utils.IsFatal(e))
                {
                    this.host.OnFatalExceptionObserved(e);
                }
            }
            finally
            {
                // we don't need the contents of the stream anymore.
                this.stream.SetLength(0);
            }

            // Confirm all sent events, and retry or report maybe-sent ones
            List<Event> requeue = null;

            try
            {
                int confirmed = 0;
                int requeued = 0;
                int dropped = 0;

                for (int i = 0; i < toSend.Count; i++)
                {
                    var evt = toSend[i];

                    if (i <= sentSuccessfully)
                    {
                        // the event was definitely sent successfully
                        DurabilityListeners.ConfirmDurable(evt);
                        confirmed++;
                    }
                    else if (i > maybeSent || evt.SafeToRetryFailedSend())
                    {
                        // the event was definitely not sent, OR it was maybe sent but can be duplicated safely
                        (requeue ?? (requeue = new List<Event>())).Add(evt);
                        requeued++;
                    }
                    else
                    {
                        // the event may have been sent or maybe not, report problem to listener
                        // this is used only on clients, who can give the exception back to the caller
                        DurabilityListeners.ReportException(evt, senderException);
                        dropped++;
                    }
                }

                if (requeue != null)
                {
                    // take a deep breath before trying again
                    await Task.Delay(this.backoff).ConfigureAwait(false);

                    this.Requeue(requeue);
                }

                if (requeued > 0 || dropped > 0)
                    this.traceHelper.LogWarning("EventHubsSender {eventHubName}/{eventHubPartitionId} has confirmed {confirmed}, requeued {requeued}, dropped {dropped} outbound events", this.eventHubName, this.eventHubPartition, confirmed, requeued, dropped);
                else
                    this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} has confirmed {confirmed}, requeued {requeued}, dropped {dropped} outbound events", this.eventHubName, this.eventHubPartition, confirmed, requeued, dropped);
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("EventHubsSender {eventHubName}/{eventHubPartitionId} encountered an error while trying to confirm messages: {exception}", this.eventHubName, this.eventHubPartition, exception);

                if (Utils.IsFatal(exception))
                {
                    this.host.OnFatalExceptionObserved(exception);
                }
            }
        }
    }
*/
}