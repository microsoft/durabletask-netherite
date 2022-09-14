namespace DurableTask.Netherite.CustomTransport
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Azure;
    using Microsoft.Extensions.Logging;

    class ClientInfo : BatchWorker<ClientEvent>, TransportAbstraction.ISender
    {
        readonly CustomTransport transport;
        readonly PartitionSender[] partitionSenders;
        readonly TransportAbstraction.IClient client;

        public Guid ClientId => this.client.ClientId;

        public ClientInfo(Guid clientId, CustomTransport transport)
            : base($"ClientWorker{clientId:N}", false, 100, CancellationToken.None, null)
        {
            this.transport = transport;
            this.client = this.transport.Host.AddClient(clientId, this.transport.Parameters.TaskhubGuid, this);
            this.partitionSenders = Enumerable
                .Range(0, transport.Parameters.PartitionCount)
                .Select(i => new PartitionSender(i, transport))
                .ToArray();
        }

        public async Task StopAsync()
        {
            await this.client.StopAsync();
        }

        void TransportAbstraction.ISender.Submit(Event element)
        {
            switch (element)
            {
                case PartitionEvent partitionEvent:
                    this.partitionSenders[partitionEvent.PartitionId].Submit(partitionEvent);
                    break;

                case ClientEvent clientEvent:
                case LoadMonitorEvent loadMonitorEvent:
                    throw new Exception("unexpected destination");
            }
        }

        public void Deliver(Stream stream)
        {
            var clientEvents = Serializer.DeserializeClientBatch(stream);
            this.SubmitBatch(clientEvents);
        }

        protected override Task Process(IList<ClientEvent> batch)
        {
            foreach(var clientEvent in batch)
            {
                Debug.Assert(clientEvent.ClientId == this.ClientId);
                this.client.Process(clientEvent);
            }
            return Task.CompletedTask;
        }
    }
}
