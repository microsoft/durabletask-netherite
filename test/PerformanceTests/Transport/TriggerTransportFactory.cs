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
        static TriggerTransport transport;

        public ITransportLayer Create(NetheriteOrchestrationService orchestrationService)
        {
            transport = new TriggerTransport(orchestrationService);
            return transport;
        }

        public static TriggerTransport Instance => transport;
    }
}
