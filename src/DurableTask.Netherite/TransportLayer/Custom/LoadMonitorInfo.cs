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
    using Microsoft.Extensions.Logging;

    class LoadMonitorInfo : BatchWorker<LoadMonitorEvent>, TransportAbstraction.ISender
    {
        readonly CustomTransport transport;
        readonly PartitionSender[] partitionSenders;
        readonly TransportAbstraction.ILoadMonitor loadMonitor;

        public LoadMonitorInfo(CustomTransport transport)
            : base($"LoadMonitorWorker", false, 100, CancellationToken.None, null)
        {
            this.transport = transport;
            this.loadMonitor = this.transport.Host.AddLoadMonitor(this.transport.Parameters.TaskhubGuid, this);
            this.partitionSenders = Enumerable
                .Range(0, transport.Parameters.PartitionCount)
                .Select(i => new PartitionSender(i, transport))
                .ToArray();
        }

        public async Task StopAsync()
        {
            await this.loadMonitor.StopAsync();
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
            var loadMonitorEvents = Serializer.DeserializeLoadMonitorBatch(stream);
            this.SubmitBatch(loadMonitorEvents);
        }

        protected override Task Process(IList<LoadMonitorEvent> batch)
        {
            foreach (var loadMonitorEvent in batch)
            {
                this.loadMonitor.Process(loadMonitorEvent);
            }
            return Task.CompletedTask;
        }
    }
}
