// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
        readonly ILogger logger;

        // we are pushing the aggregated load information on a somewhat slower interval
        public static TimeSpan AggregatePublishInterval = TimeSpan.FromSeconds(15);
        readonly CancellationTokenSource cancelWait = new CancellationTokenSource();

        public LoadPublisher(ILoadMonitorService service, CancellationToken token, ILogger logger) : base(nameof(LoadPublisher), false, token)
        {
            this.service = service;
            this.logger = logger;
            this.cancelWait = new CancellationTokenSource();
        }

        public void Flush()
        {
            this.cancelWait.Cancel(); // so that we don't have to wait the whole delay
        }

        protected override async Task Process(IList<(uint, PartitionLoadInfo)> batch)
        {
            try
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
                    catch
                    {
                        // we swallow exceptions so we can tolerate temporary Azure storage errors
                        // TODO log
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
            catch(Exception e)
            {
                this.logger.LogWarning("Could not publish load {exception}", e);
            }
        }
    }
}
 