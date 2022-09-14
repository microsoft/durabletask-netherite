namespace DurableTask.Netherite.CustomTransport
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    class LoadMonitorSender : DurableTask.Netherite.BatchWorker<LoadMonitorEvent>
    {
        readonly CustomTransport transport;

        public LoadMonitorSender(CustomTransport transport)
            : base($"LoadMonitorSender", false, 500, CancellationToken.None, null)
        {
            this.transport = transport;
        }

        protected override async Task Process(IList<LoadMonitorEvent> batch)
        {
            try
            {
                using var stream = new MemoryStream();
                Serializer.SerializeLoadMonitorBatch((List<LoadMonitorEvent>)batch, stream);
                stream.Seek(0, SeekOrigin.Begin);
                await this.transport.SendToLoadMonitorAsync(stream);
            }
            catch (Exception e)
            {
                foreach (var evt in batch)
                {
                    DurabilityListeners.ReportException(evt, e);
                }

                throw;
            }

            foreach (var e in batch)
            {
                DurabilityListeners.ConfirmDurable(e);
            }
        }
    }
}