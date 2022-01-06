// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Emulated
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    class SendWorker : BatchWorker<Event>, TransportAbstraction.ISender
    {
        Action<IEnumerable<Event>> sendHandler;

        public SendWorker(CancellationToken token)
            : base(nameof(SendWorker), true, int.MaxValue, token, null)
        {
        }

        public void SetHandler(Action<IEnumerable<Event>> sendHandler)
        {
            this.sendHandler = sendHandler ?? throw new ArgumentNullException(nameof(sendHandler));
        }

        void TransportAbstraction.ISender.Submit(Event element)
        {
            this.Submit(element);
        }

        protected override Task Process(IList<Event> batch)
        {
            if (batch.Count > 0)
            {
                try
                {
                    this.sendHandler(batch);
                }
                catch (Exception e)
                {
                    System.Diagnostics.Trace.TraceError($"exception in send worker: {e}", e);
                }
            }

            return Task.CompletedTask;
        }
    }
}
