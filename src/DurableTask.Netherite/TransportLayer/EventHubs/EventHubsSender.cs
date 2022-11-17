// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using DurableTask.Core.Common;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
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
        readonly TimeSpan backoff = TimeSpan.FromSeconds(5);
        int maxMessageSize = 900 * 1024; // we keep this slightly below the official limit since we have observed exceptions
        int maxFragmentSize => this.maxMessageSize / 2; // we keep this lower than maxMessageSize because of serialization overhead
        readonly MemoryStream stream = new MemoryStream(); // reused for all packets
        readonly Stopwatch stopwatch = new Stopwatch();

        public EventHubsSender(TransportAbstraction.IHost host, byte[] taskHubGuid, PartitionSender sender, EventHubsTraceHelper traceHelper)
            : base($"EventHubsSender {sender.EventHubClient.EventHubName}/{sender.PartitionId}", false, 2000, CancellationToken.None, traceHelper)
        {
            this.host = host;
            this.taskHubGuid = taskHubGuid;
            this.sender = sender;
            this.traceHelper = traceHelper;
            this.eventHubName = this.sender.EventHubClient.EventHubName;
            this.eventHubPartition = this.sender.PartitionId;
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

            EventDataBatch CreateBatch() => this.sender.CreateBatch(new BatchOptions() { MaxMessageSize = maxMessageSize });

            try
            {
                var batch = CreateBatch();

                async Task SendBatch(int lastPosition)
                {
                    maybeSent = lastPosition;
                    this.stopwatch.Restart();
                    await this.sender.SendAsync(batch).ConfigureAwait(false);
                    this.stopwatch.Stop();
                    sentSuccessfully = lastPosition;
                    this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} sent batch of {numPackets} packets ({size} bytes) in {latencyMs:F2}ms, throughput={throughput:F2}MB/s", this.eventHubName, this.eventHubPartition, batch.Count, batch.Size, this.stopwatch.Elapsed.TotalMilliseconds, batch.Size/(1024*1024*this.stopwatch.Elapsed.TotalSeconds));
                    batch.Dispose();
                }

                for (int i = 0; i < toSend.Count; i++)
                {
                    long startPos = this.stream.Position;
                    var evt = toSend[i];

                    this.traceHelper.LogTrace("EventHubsSender {eventHubName}/{eventHubPartitionId} is sending event {evt} id={eventId}", this.eventHubName, this.eventHubPartition, evt, evt.EventIdString);
                    Packet.Serialize(evt, this.stream, this.taskHubGuid);
                    int length = (int)(this.stream.Position - startPos);
                    var arraySegment = new ArraySegment<byte>(this.stream.GetBuffer(), (int)startPos, length);
                    var eventData = new EventData(arraySegment);
                    bool tooBig = length > this.maxFragmentSize;

                    if (!tooBig && batch.TryAdd(eventData))
                    {
                        this.traceHelper.LogTrace("EventHubsSender {eventHubName}/{eventHubPartitionId} added packet to batch ({size} bytes) {evt} id={eventId}", this.eventHubName, this.eventHubPartition, eventData.Body.Count, evt, evt.EventIdString);
                        continue;
                    }
                    else
                    {
                        if (batch.Count > 0)
                        {
                            // send the batch we have so far
                            await SendBatch(i - 1);

                            // create a fresh batch
                            batch = CreateBatch();
                        }

                        if (tooBig)
                        {
                            // the message is too big. Break it into fragments, and send each individually.
                            this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} fragmenting large event ({size} bytes) id={eventId}", this.eventHubName, this.eventHubPartition, length, evt.EventIdString);
                            var fragments = FragmentationAndReassembly.Fragment(arraySegment, evt, this.maxFragmentSize);
                            maybeSent = i;
                            for (int k = 0; k < fragments.Count; k++)
                            {
                                //TODO send bytes directly instead of as events (which causes significant space overhead)
                                this.stream.Seek(0, SeekOrigin.Begin);
                                var fragment = fragments[k];
                                Packet.Serialize((Event)fragment, this.stream, this.taskHubGuid);
                                this.traceHelper.LogTrace("EventHubsSender {eventHubName}/{eventHubPartitionId} sending fragment {index}/{total} ({size} bytes) id={eventId}", this.eventHubName, this.eventHubPartition, k, fragments.Count, length, ((Event)fragment).EventIdString);
                                length = (int)this.stream.Position;
                                await this.sender.SendAsync(new EventData(new ArraySegment<byte>(this.stream.GetBuffer(), 0, length))).ConfigureAwait(false);
                                this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} sent fragment {index}/{total} ({size} bytes) id={eventId}", this.eventHubName, this.eventHubPartition, k, fragments.Count, length, ((Event)fragment).EventIdString);
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
                    await SendBatch(toSend.Count - 1);
                    
                    // the buffer can be reused now
                    this.stream.Seek(0, SeekOrigin.Begin);
                }
            }
            catch(Microsoft.Azure.EventHubs.MessageSizeExceededException)
            {
                this.maxMessageSize = 200 * 1024;
                this.traceHelper.LogWarning("EventHubsSender {eventHubName}/{eventHubPartitionId} failed to send due to message size, reducing to {maxMessageSize}kB",
                    this.eventHubName, this.eventHubPartition, this.maxMessageSize / 1024);
            }
            catch (Exception e)
            {
                this.traceHelper.LogWarning(e, "EventHubsSender {eventHubName}/{eventHubPartitionId} failed to send", this.eventHubName, this.eventHubPartition);
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
                    this.traceHelper.LogWarning("EventHubsSender {eventHubName}/{eventHubPartitionId} has confirmed {confirmed}, requeued {requeued}, dropped {dropped} outbound events", this.eventHubName, this.eventHubPartition, confirmed, requeued, dropped);
                else
                    this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} has confirmed {confirmed}, requeued {requeued}, dropped {dropped} outbound events", this.eventHubName, this.eventHubPartition, confirmed, requeued, dropped);
            }
            catch (Exception exception) when (!Utils.IsFatal(exception))
            {
                this.traceHelper.LogError("EventHubsSender {eventHubName}/{eventHubPartitionId} encountered an error while trying to confirm messages: {exception}", this.eventHubName, this.eventHubPartition, exception);
            }
        }
    }
}
