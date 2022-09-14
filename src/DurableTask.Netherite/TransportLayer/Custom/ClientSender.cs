namespace DurableTask.Netherite.CustomTransport
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    class ClientSender : DurableTask.Netherite.BatchWorker<ClientEvent>
    {
        readonly Guid clientId;
        readonly CustomTransport transport;

        public ClientSender(Guid clientId, CustomTransport transport)
            : base($"ClientSender{clientId:N}", false, 500, CancellationToken.None, null)
        {
            this.clientId = clientId;
            this.transport = transport;
        }

        protected override async Task Process(IList<ClientEvent> batch)
        {
            try
            {
                using var stream = new MemoryStream();
                Serializer.SerializeClientBatch((List<ClientEvent>)batch, stream);
                stream.Seek(0, SeekOrigin.Begin);
                await this.transport.SendToClientAsync(this.clientId, stream);
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