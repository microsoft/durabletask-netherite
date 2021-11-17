// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Scaling
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    class LoadPublisher : BatchWorker<(uint, PartitionLoadInfo)>
    {
        readonly ILoadMonitorService service;
        readonly OrchestrationServiceTraceHelper traceHelper;

        // we are pushing the aggregated load information on a somewhat slower interval
        public static TimeSpan AggregatePublishInterval = TimeSpan.FromSeconds(2);
        readonly CancellationTokenSource cancelWait = new CancellationTokenSource();

        public LoadPublisher(ILoadMonitorService service, CancellationToken token, OrchestrationServiceTraceHelper traceHelper) : base(nameof(LoadPublisher), false, int.MaxValue, token, null)
        {
            this.service = service;
            this.traceHelper = traceHelper;
            this.cancelWait = new CancellationTokenSource();
        }

        public Task FlushAsync()
        {
            this.cancelWait.Cancel(); // so that we don't have to wait the whole delay
            return this.WaitForCompletionAsync();
        }

        protected override async Task Process(IList<(uint, PartitionLoadInfo)> batch)
        {
            if (batch.Count != 0)
            {
                var latestForEachPartition = new Dictionary<uint, PartitionLoadInfo>();

                foreach (var (partitionId, info) in batch)
                {
                    latestForEachPartition[partitionId] = info;
                }

                try
                {
                    await this.service.PublishAsync(latestForEachPartition, this.cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // o.k. during shutdown
                }
                catch (Exception exception)
                {
                    // we swallow exceptions so we can tolerate temporary Azure storage errors
                    this.traceHelper.TraceError("LoadPublisher failed", exception);
                }
            }

            try
            {
                await Task.Delay(AggregatePublishInterval, this.cancelWait.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }
}
 