// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using Azure.Messaging.EventHubs;
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    class EventHubsClientSender
    {
        readonly EventHubsSender<ClientEvent>[] channels;
        int roundRobin;

        public EventHubsClientSender(TransportAbstraction.IHost host, Guid clientId, (EventHubConnection connection, string partitionId)[] partitions, CancellationToken shutdownToken, EventHubsTraceHelper traceHelper, NetheriteOrchestrationServiceSettings settings)
        {
            this.channels = new Netherite.EventHubsTransport.EventHubsSender<ClientEvent>[partitions.Length];
            for (int i = 0; i < partitions.Length; i++)
            {
                this.channels[i] = new EventHubsSender<ClientEvent>(host, clientId.ToByteArray(), partitions[i].connection, partitions[i].partitionId, shutdownToken, traceHelper, settings);
            }
        }

        EventHubsSender<ClientEvent> NextChannel()
        {
            this.roundRobin = (this.roundRobin + 1) % this.channels.Length;
            return this.channels[this.roundRobin];
        }

        bool Idle(EventHubsSender<ClientEvent> sender) => sender.IsIdle;

        public void Submit(ClientEvent toSend)
        {
            var channel = this.channels.FirstOrDefault(this.Idle) ?? this.NextChannel();
            channel.Submit(toSend);
        }

        public Task WaitForShutdownAsync()
        {
            return Task.WhenAll(this.channels.Select(sender => sender.WaitForShutdownAsync()).ToList());
        }
    }
}
