// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.SingleHostTransport
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using static DurableTask.Netherite.TransportAbstraction;

    /// <summary>
    /// An in-memory queue for delivering events.
    /// </summary>
    class LoadMonitorQueue : BatchWorker<LoadMonitorEvent>
    {
        public TransportAbstraction.ILoadMonitor LoadMonitor { get; }

        public LoadMonitorQueue(TransportAbstraction.ILoadMonitor loadMonitor, ILogger logger)
            : base("LoadMonitorQueue", false, int.MaxValue, CancellationToken.None, null)
        {
            this.LoadMonitor = loadMonitor;
        }

        protected override Task Process(IList<LoadMonitorEvent> batch)
        {
            try
            {
                foreach (var evt in batch)
                {
                    this.LoadMonitor.Process(evt);
                    DurabilityListeners.ConfirmDurable(evt);
                }
            }
            catch (System.Threading.Tasks.TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception)
            {
            }

            return Task.CompletedTask;
        }
    }
}