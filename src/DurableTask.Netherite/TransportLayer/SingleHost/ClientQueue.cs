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

    /// <summary>
    /// An in-memory queue for delivering events.
    /// </summary>
    class ClientQueue : BatchWorker<ClientEvent>
    {
        public TransportAbstraction.IClient Client { get; }

        public ClientQueue(TransportAbstraction.IClient client, ILogger logger)
            : base($"ClientQueue.{Netherite.Client.GetShortId(client.ClientId)}", false, int.MaxValue, CancellationToken.None, null)
        {
            this.Client = client;
        }

        protected override Task Process(IList<ClientEvent> batch)
        {         
            try
            {
                foreach (var evt in batch)
                {
                    this.Client.Process(evt);
                    DurabilityListeners.ConfirmDurable(evt);
                }
            }
            catch (System.Threading.Tasks.TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                this.Client.ReportTransportError(nameof(ClientQueue), e);
            }

            return Task.CompletedTask;
        }
    }
}
