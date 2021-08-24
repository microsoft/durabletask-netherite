// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Emulated
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.IO;
    using System.Threading;

    /// <summary>
    /// Simulates a in-memory queue for delivering events. Used for local testing and debugging.
    /// </summary>
    class MemoryLoadMonitorQueue : MemoryQueue<LoadMonitorEvent, byte[]>, IMemoryQueue<LoadMonitorEvent>
    {
        readonly TransportAbstraction.ILoadMonitor loadMonitor;

        public MemoryLoadMonitorQueue(TransportAbstraction.ILoadMonitor loadMonitor, CancellationToken cancellationToken, ILogger logger)
            : base(cancellationToken, $"LoadMonitor", logger)
        {
            this.loadMonitor = loadMonitor;
        }

        protected override byte[] Serialize(LoadMonitorEvent evt)
        {
            var stream = new MemoryStream();
            Packet.Serialize(evt, stream, new byte[16]);
            return stream.ToArray();
        }

        protected override LoadMonitorEvent Deserialize(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes, false))
            {
                Packet.Deserialize(stream, out LoadMonitorEvent clientEvent, null);
                return clientEvent;
            }
        }

        protected override void Deliver(LoadMonitorEvent evt)
        {
            try
            {
                this.loadMonitor.Process(evt);
            }
            catch (System.Threading.Tasks.TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception)
            {
            }
        }
    }
}
