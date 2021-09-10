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
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    class EventHubsClientSender
    {
        readonly EventHubsSender<ClientEvent>[] channels;
        int roundRobin;

        public EventHubsClientSender(TransportAbstraction.IHost host, byte[] taskHubGuid, Guid clientId, PartitionSender[] senders, EventHubsTraceHelper traceHelper)
        {
            this.channels = new EventHubs.EventHubsSender<ClientEvent>[senders.Length];
            for (int i = 0; i < senders.Length; i++)
            {
                this.channels[i] = new EventHubsSender<ClientEvent>(host, taskHubGuid, senders[i], traceHelper);
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
    }
}
