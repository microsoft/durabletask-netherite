// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Remp
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Microsoft.Azure.Documents.SystemFunctions;
    using static RempFormat;

    public class RempSinkWithErrorHandler : IListener
    {
        readonly IListener wrapped;
        readonly Action<Exception> handler;

        public RempSinkWithErrorHandler(IListener wrapped, Action<Exception> handler)
        {
            this.wrapped = wrapped;
            this.handler = handler;
        }

        public void WorkerHeader(string workerId, IEnumerable<WorkitemGroup> groups)
        {
            try
            {
                this.wrapped.WorkerHeader(workerId, groups);
            }
            catch (Exception e)
            {
                this.handler?.Invoke(e);
            }
        }

        public void WorkItem(long timeStamp, string workItemId, int group, double latencyMs, IEnumerable<NamedPayload> consumedMessages, IEnumerable<NamedPayload> producedMessages, bool allowSpeculation, InstanceState? instanceState)
        {
            try
            {
                this.wrapped.WorkItem(timeStamp, workItemId, group, latencyMs, consumedMessages, producedMessages, allowSpeculation, instanceState);
            }
            catch (Exception e)
            {
                this.handler?.Invoke(e);
            }
        }
    }
}
