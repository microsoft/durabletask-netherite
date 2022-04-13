// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubs
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

    class LoadMonitorSender : BatchWorker<LoadMonitorEvent>
    {
        readonly PartitionSender sender;
        readonly TransportAbstraction.IHost host;
        readonly byte[] taskHubGuid;
        readonly EventHubsTraceHelper traceHelper;
        readonly string eventHubName;
        readonly string eventHubPartition;
        readonly TimeSpan backoff = TimeSpan.FromSeconds(5);
        const int maxFragmentSize = 500 * 1024; // account for very non-optimal serialization of event
        readonly MemoryStream stream = new MemoryStream(); // reused for all packets
        readonly Stopwatch stopwatch = new Stopwatch();

        public LoadMonitorSender(TransportAbstraction.IHost host, byte[] taskHubGuid, PartitionSender sender, EventHubsTraceHelper traceHelper)
            : base($"EventHubsSender {sender.EventHubClient.EventHubName}/{sender.PartitionId}", false, 2000, CancellationToken.None, traceHelper)
        {
            this.host = host;
            this.taskHubGuid = taskHubGuid;
            this.sender = sender;
            this.traceHelper = traceHelper;
            this.eventHubName = this.sender.EventHubClient.EventHubName;
            this.eventHubPartition = this.sender.PartitionId;
        }

        protected override async Task Process(IList<LoadMonitorEvent> toSend)
        {
            if (toSend.Count == 0)
            {
                return;
            }

            // this batch worker performs some functions that are specific to the load monitor
            // - filters the sent events so only the most recent info is sent
            // - does rate limiting 
            
            try
            {

                bool[] sentLoadInformationReceived = new bool[32];
                bool[] sentPositionsReceived = new bool[32];

                this.stopwatch.Restart();
                int numEvents = 0;

                for (int i = toSend.Count - 1; i >= 0; i--)
                {
                    var evt = toSend[i];

                    // send only the most recent packet from each partition
                    if (evt is LoadInformationReceived loadInformationReceived)
                    {
                        if (sentLoadInformationReceived[loadInformationReceived.PartitionId])
                        {
                            continue;
                        }
                        else
                        {
                            sentLoadInformationReceived[loadInformationReceived.PartitionId] = true;
                        }
                    }
                    else if (evt is PositionsReceived positionsReceived)
                    {
                        if (sentPositionsReceived[positionsReceived.PartitionId])
                        {
                            continue;
                        }
                        else
                        {
                            sentPositionsReceived[positionsReceived.PartitionId] = true;
                        }
                    }

                    Packet.Serialize(evt, this.stream, this.taskHubGuid);
                    int length = (int)(this.stream.Position);
                    var arraySegment = new ArraySegment<byte>(this.stream.GetBuffer(), 0, length);
                    var eventData = new EventData(arraySegment);
                    await this.sender.SendAsync(eventData);
                    this.traceHelper.LogTrace("EventHubsSender {eventHubName}/{eventHubPartitionId} sent packet ({size} bytes) id={eventId}", this.eventHubName, this.eventHubPartition, length, evt.EventIdString);
                    this.stream.Seek(0, SeekOrigin.Begin);
                    numEvents++;
                }

                long elapsed = this.stopwatch.ElapsedMilliseconds;
                this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} sent info numEvents={numEvents} latencyMs={latencyMs}", this.eventHubName, this.eventHubPartition, numEvents, elapsed);
                
                // rate limit this sender by making each iteration last at least 100ms duration
                long toSpare = 100 - elapsed;
                if (toSpare > 10)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(toSpare));
                    this.traceHelper.LogTrace("EventHubsSender {eventHubName}/{eventHubPartitionId} iteration padded to latencyMs={latencyMs}", this.eventHubName, this.eventHubPartition, this.stopwatch.ElapsedMilliseconds);
                }
            }
            catch (Exception e)
            {
                this.traceHelper.LogWarning(e, "EventHubsSender {eventHubName}/{eventHubPartitionId} failed to send", this.eventHubName, this.eventHubPartition);
            }
            finally
            {
                // we don't need the contents of the stream anymore.
                this.stream.SetLength(0);
            }
        }
    }
}
