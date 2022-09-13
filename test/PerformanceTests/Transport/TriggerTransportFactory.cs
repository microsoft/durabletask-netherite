namespace PerformanceTests.Transport
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Netherite;

    public class TriggerTransportFactory : ITransportLayerFactory
    {
        TriggerTransport transport;

        public ITransportLayer Create(NetheriteOrchestrationService orchestrationService)
        {
            this.transport = new TriggerTransport(orchestrationService);
            return this.transport;
        }

        public TriggerTransport Instance => this.transport;
    }
}
