// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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

    class WorkerSender : BatchWorker<WorkerEvent>
    {
        readonly EventHubClient client;
        readonly TransportAbstraction.IWorker fallbackWorker;
        readonly TransportAbstraction.IHost host;
        readonly byte[] taskHubGuid;
        readonly EventHubsTraceHelper traceHelper;
        readonly string eventHubName;
        readonly TimeSpan backoff = TimeSpan.FromSeconds(5);
        readonly MemoryStream stream = new MemoryStream(); // reused for all packets

        public WorkerSender(TransportAbstraction.IHost host, byte[] taskHubGuid, EventHubClient client, TransportAbstraction.IWorker fallback, EventHubsTraceHelper traceHelper)
            : base(nameof(EventHubsSender<WorkerEvent>), false, 2000, CancellationToken.None, null)
        {
            this.host = host;
            this.taskHubGuid = taskHubGuid;
            this.client = client;
            this.fallbackWorker = fallback;
            this.traceHelper = traceHelper;
            this.eventHubName = this.client.EventHubName;
        }

        protected override async Task Process(IList<WorkerEvent> toSend)
        {
            if (toSend.Count == 0)
            {
                return;
            }

            // track progress in case of exception
            var sentSuccessfully = -1;
            var maybeSent = -1;
            HashSet<int> fallbacks = new HashSet<int>();

            try
            {
                // we manually set the max message size to leave extra room as
                // we have observed exceptions in practice otherwise.
                var batch = this.client.CreateBatch(new BatchOptions() { MaxMessageSize = 900 * 1024 });

                for (int i = 0; i < toSend.Count; i++)
                {
                    long startPos = this.stream.Position;
                    var evt = toSend[i];
                    Packet.Serialize(evt, this.stream, this.taskHubGuid);
                    int length = (int)(this.stream.Position - startPos);
                    var arraySegment = new ArraySegment<byte>(this.stream.GetBuffer(), (int)startPos, length);
                    var eventData = new EventData(arraySegment);
                    bool tooBig = length > 800 * 1024;

                    if (!tooBig && batch.TryAdd(eventData))
                    {
                        this.traceHelper.LogTrace("EventHubsSender {eventHubName} added packet to batch ({size} bytes) {evt} id={eventId}", this.eventHubName, eventData.Body.Count, evt, evt.EventIdString);
                        continue;
                    }
                    else
                    {
                        if (batch.Count > 0)
                        {
                            // send the batch we have so far
                            maybeSent = i - 1;
                            await this.client.SendAsync(batch).ConfigureAwait(false);
                            sentSuccessfully = i - 1;

                            this.traceHelper.LogDebug("EventHubsSender {eventHubName} sent batch of {numPackets} packets", this.eventHubName, batch.Count);

                            // create a fresh batch
                            batch = this.client.CreateBatch();
                        }

                        if (tooBig)
                        {
                            // the message is too big. We will return it to the local worker.
                            fallbacks.Add(i);
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
                    await this.client.SendAsync(batch).ConfigureAwait(false);
                    sentSuccessfully = toSend.Count - 1;

                    this.traceHelper.LogDebug("EventHubsSender {eventHubName} sent batch of {numPackets} packets", this.eventHubName, batch.Count);

                    // the buffer can be reused now
                    this.stream.Seek(0, SeekOrigin.Begin);
                }
            }
            catch (Exception e)
            {
                this.traceHelper.LogWarning(e, "EventHubsSender {eventHubName} failed to send", this.eventHubName);
            }
            finally
            {
                // we don't need the contents of the stream anymore.
                this.stream.SetLength(0);
            }

            // Confirm all sent events, and retry or report maybe-sent ones
            List<WorkerEvent> requeue = null;

            try
            {
                int confirmed = 0;
                int fallback = 0;
                int requeued = 0;

                for (int i = 0; i < toSend.Count; i++)
                {
                    var evt = toSend[i];

                    if (i <= sentSuccessfully)
                    {
                        if (!fallbacks.Contains(i))
                        {
                            // the event was definitely sent successfully
                            DurabilityListeners.ConfirmDurable(evt);
                            confirmed++;
                        }
                        else
                        {
                            await this.fallbackWorker.SubmitAsync(evt, null);
                            fallback++;
                        }
                    }
                    else if (i > maybeSent)
                    {
                        // the event was not definitely sent so we requeue it
                        (requeue ?? (requeue = new List<WorkerEvent>())).Add(evt);
                        requeued++;
                    }
                }

                if (requeue != null)
                {
                    // take a deep breath before trying again
                    await Task.Delay(this.backoff).ConfigureAwait(false);

                    this.Requeue(requeue);
                }

                if (requeued > 0 || fallback > 0)
                    this.traceHelper.LogWarning("EventHubsSender {eventHubName} has confirmed {confirmed}, requeued {requeued}, fallback {fallback} outbound events", this.eventHubName, confirmed, requeued, fallback);
                else
                    this.traceHelper.LogDebug("EventHubsSender {eventHubName} has confirmed {confirmed}, requeued {requeued}, fallback {fallback} outbound events", this.eventHubName, confirmed, requeued, fallback);
            }
            catch (Exception exception) when (!Utils.IsFatal(exception))
            {
                this.traceHelper.LogError("EventHubsSender {eventHubName} encountered an error while trying to confirm messages: {exception}", this.eventHubName, exception);
            }
        }
    }
}
