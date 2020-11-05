// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.EventHubs
{
    using DurableTask.Core.Common;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    class EventHubsSender<T> : BatchWorker<Event> where T: Event
    {
        readonly PartitionSender sender;
        readonly TransportAbstraction.IHost host;
        readonly byte[] taskHubGuid;
        readonly EventHubsTraceHelper traceHelper;
        readonly string eventHubName;
        readonly string eventHubPartition;
        readonly bool useJsonPackets;
        readonly TimeSpan backoff = TimeSpan.FromSeconds(5);
        const int maxFragmentSize = 500 * 1024; // account for very non-optimal serialization of event
        readonly MemoryStream stream = new MemoryStream(); // reused for all packets

        public EventHubsSender(TransportAbstraction.IHost host, byte[] taskHubGuid, PartitionSender sender, EventHubsTraceHelper traceHelper, bool useJsonPackets)
            : base(nameof(EventHubsSender<T>), false, CancellationToken.None)
        {
            this.host = host;
            this.taskHubGuid = taskHubGuid;
            this.sender = sender;
            this.traceHelper = traceHelper;
            this.eventHubName = this.sender.EventHubClient.EventHubName;
            this.eventHubPartition = this.sender.PartitionId;
            this.useJsonPackets = useJsonPackets;
        }

        protected override void WorkLoopCompleted(int batchSize, double elapsedMilliseconds, int? nextBatch)
        {
            this.traceHelper.LogDebug($"EventHubsSender completed batch: batchSize={batchSize} elapsedMilliseconds={elapsedMilliseconds} nextBatch={nextBatch}");
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
            
            try
            {
                // we manually set the max message size to leave extra room as
                // we have observed exceptions in practice otherwise.
                var batch = this.sender.CreateBatch(new BatchOptions() { MaxMessageSize = 900 * 1024 });

                for (int i = 0; i < toSend.Count; i++)
                {
                    long startPos = this.stream.Position;
                    var evt = toSend[i];
                    Packet.Serialize(evt, this.stream, this.useJsonPackets, this.taskHubGuid);
                    int length = (int)(this.stream.Position - startPos);
                    var arraySegment = new ArraySegment<byte>(this.stream.GetBuffer(), (int)startPos, length);
                    var eventData = new EventData(arraySegment);
                    bool tooBig = length > maxFragmentSize;

                    if (!tooBig && batch.TryAdd(eventData))
                    {
                        this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} added packet to batch ({size} bytes) {evt} id={eventId}", this.eventHubName, this.eventHubPartition, eventData.Body.Count, evt, evt.EventIdString);
                        continue;
                    }
                    else
                    {
                        if (batch.Count > 0)
                        {
                            // send the batch we have so far
                            maybeSent = i - 1;
                            await this.sender.SendAsync(batch).ConfigureAwait(false);
                            sentSuccessfully = i - 1;

                            this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} sent batch of {numPackets} packets", this.eventHubName, this.eventHubPartition, batch.Count);

                            // create a fresh batch
                            batch = this.sender.CreateBatch();
                        }

                        if (tooBig)
                        {
                            // the message is too big. Break it into fragments, and send each individually.
                            var fragments = FragmentationAndReassembly.Fragment(arraySegment, evt, maxFragmentSize);
                            maybeSent = i;
                            foreach (var fragment in fragments)
                            {
                                //TODO send bytes directly instead of as events (which causes significant space overhead)
                                this.stream.Seek(0, SeekOrigin.Begin);
                                Packet.Serialize((Event)fragment, this.stream, this.useJsonPackets, this.taskHubGuid);
                                length = (int)this.stream.Position;
                                await this.sender.SendAsync(new EventData(new ArraySegment<byte>(this.stream.GetBuffer(), 0, length))).ConfigureAwait(false);
                                this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} sent packet ({size} bytes) {evt} id={eventId}", this.eventHubName, this.eventHubPartition, length, fragment, ((Event)fragment).EventIdString);
                            }
                            sentSuccessfully = i;
                        }
                        else
                        {
                            // back up one
                            i--;
                        }

                        // the buffer can be reused now
                        this.stream.Seek(0, SeekOrigin.Begin);
                    }
                }

                if (batch.Count > 0)
                {
                    maybeSent = toSend.Count - 1;
                    await this.sender.SendAsync(batch).ConfigureAwait(false);
                    sentSuccessfully = toSend.Count - 1;

                    this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} sent batch of {numPackets} packets", this.eventHubName, this.eventHubPartition, batch.Count);

                    // the buffer can be reused now
                    this.stream.Seek(0, SeekOrigin.Begin);
                }
            }
            catch (Exception e)
            {
                this.traceHelper.LogWarning(e, "EventHubsSender {eventHubName}/{eventHubPartitionId} failed to send", this.eventHubName, this.eventHubPartition, this.sender.EventHubClient.EventHubName, this.sender.PartitionId);
                senderException = e;
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
                    this.traceHelper.LogWarning("EventHubsSender {eventHubName}/{eventHubPartitionId} has confirmed {confirmed}, requeued {requeued}, dropped {dropped} outbound events", this.eventHubName, this.eventHubPartition, confirmed, requeued, dropped, this.sender.EventHubClient.EventHubName, this.sender.PartitionId);
                else
                    this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} has confirmed {confirmed}, requeued {requeued}, dropped {dropped} outbound events", this.eventHubName, this.eventHubPartition, confirmed, requeued, dropped, this.sender.EventHubClient.EventHubName, this.sender.PartitionId);
            }
            catch (Exception exception) when (!Utils.IsFatal(exception))
            {
                this.traceHelper.LogError("EventHubsSender {eventHubName}/{eventHubPartitionId} encountered an error while trying to confirm messages: {exception}", this.eventHubName, this.eventHubPartition, exception);
            }
        }
    }
}
