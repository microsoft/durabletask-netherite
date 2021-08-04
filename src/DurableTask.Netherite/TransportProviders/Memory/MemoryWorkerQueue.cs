// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Emulated
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Simulates a in-memory queue for delivering events. Used for local testing and debugging.
    /// </summary>
    class MemoryWorkerQueue : MemoryQueue<WorkerEvent, byte[]>, IMemoryQueue<WorkerEvent>
    {
        readonly TransportAbstraction.IWorker worker;

        public MemoryWorkerQueue(TransportAbstraction.IWorker worker, CancellationToken cancellationToken, ILogger logger)
            : base(cancellationToken, $"Worker.{Client.GetShortId(worker.WorkerId)}", logger)
        {
            this.worker = worker;
        }

        protected override byte[] Serialize(WorkerEvent evt)
        {
            var stream = new MemoryStream();
            Packet.Serialize(evt, stream, new byte[16]);
            return stream.ToArray();
        }

        protected override WorkerEvent Deserialize(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes, false))
            {
                Packet.Deserialize(stream, out WorkerEvent workerEvent, null);
                return workerEvent;
            }
        }

        protected override ValueTask DeliverAsync(WorkerEvent evt)
        {
            try
            {
                return this.worker.SubmitAsync(evt, null);
            }
            catch (System.Threading.Tasks.TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                this.worker.ReportTransportError(nameof(MemoryClientQueue), e);
            }

            return default;
        }
    }
}
