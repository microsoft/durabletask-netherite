//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

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

        public LoadPublisher(ILoadMonitorService service, ILogger logger) : base(nameof(LoadPublisher))
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
 