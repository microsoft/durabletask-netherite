// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Producer;
    using DurableTask.Core.Common;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    class LoadMonitorSender : BatchWorker<LoadMonitorEvent>
    {
        readonly EventHubProducerClient client;
        readonly TransportAbstraction.IHost host;
        readonly byte[] taskHubGuid;
        readonly EventHubsTraceHelper traceHelper;
        readonly string eventHubName;
        readonly string eventHubPartition;
        readonly TimeSpan backoff = TimeSpan.FromSeconds(5);
        readonly MemoryStream stream = new MemoryStream(); // reused for all packets
        readonly Stopwatch stopwatch = new Stopwatch();
        readonly List<EventData> events = new List<EventData>();

        public LoadMonitorSender(TransportAbstraction.IHost host, byte[] taskHubGuid, EventHubConnection connection, CancellationToken shutdownToken, EventHubsTraceHelper traceHelper)
            : base($"EventHubsSender {connection.EventHubName}/0", false, 2000, shutdownToken, traceHelper)
        {
            this.host = host;
            this.taskHubGuid = taskHubGuid;
            this.client = new EventHubProducerClient(connection);
            this.traceHelper = traceHelper;
            this.eventHubName = connection.EventHubName;
            this.eventHubPartition = "0";
        }

        public override async Task WaitForShutdownAsync()
        {
            await base.WaitForShutdownAsync();
            await this.client.CloseAsync(); 
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

                    int startPos = (int)this.stream.Position;
                    Packet.Serialize(evt, this.stream, this.taskHubGuid);
                    int length = (int)(this.stream.Position) - startPos;
                    var arraySegment = new ArraySegment<byte>(this.stream.GetBuffer(), startPos, length);
                    this.events.Add(new EventData(arraySegment));            
                    this.traceHelper.LogTrace("EventHubsSender {eventHubName}/{eventHubPartitionId} added packet ({size} bytes) id={eventId} to batch", this.eventHubName, this.eventHubPartition, length, evt.EventIdString);
                }

                await this.client.SendAsync(this.events, this.cancellationToken);
                long elapsed = this.stopwatch.ElapsedMilliseconds;
                this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} sent info numEvents={numEvents} latencyMs={latencyMs}", this.eventHubName, this.eventHubPartition, this.events.Count, elapsed);

                // rate limit this sender by making each iteration last at least 100ms duration
                long toSpare = 100 - elapsed;
                if (toSpare > 10)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(toSpare));
                    this.traceHelper.LogTrace("EventHubsSender {eventHubName}/{eventHubPartitionId} iteration padded to latencyMs={latencyMs}", this.eventHubName, this.eventHubPartition, this.stopwatch.ElapsedMilliseconds);
                }
            }
            catch (OperationCanceledException) when (this.cancellationToken.IsCancellationRequested)
            {
                // normal during shutdown
                this.traceHelper.LogDebug("EventHubsSender {eventHubName}/{eventHubPartitionId} was cancelled", this.eventHubName, this.eventHubPartition);
                return;
            }
            catch (Exception e)
            {
                this.traceHelper.LogWarning(e, "EventHubsSender {eventHubName}/{eventHubPartitionId} failed to send", this.eventHubName, this.eventHubPartition);
            }
            finally
            {
                // we don't need the contents of the stream anymore.
                this.stream.SetLength(0);
                this.events.Clear();
            }
        }
    }
}
