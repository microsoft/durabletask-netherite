namespace DurableTask.Netherite.CustomTransport
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    class PartitionSender : DurableTask.Netherite.BatchWorker<PartitionEvent>
    {
        readonly int partitionId;
        readonly CustomTransport transport;

        public PartitionSender(int partitionId, CustomTransport transport)
            : base($"PartitionSender{partitionId:D2}", false, 500, CancellationToken.None, null)
        {
            this.partitionId = partitionId;
            this.transport = transport;
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {
                using var stream = new MemoryStream();
                Serializer.SerializeBatch((List<PartitionEvent>)batch, stream);
                stream.Seek(0, SeekOrigin.Begin);
                await this.transport.SendToPartitionAsync(this.partitionId, stream);
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