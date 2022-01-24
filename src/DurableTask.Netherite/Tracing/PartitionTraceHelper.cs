// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using DurableTask.Core;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Extensions.Logging;

    class PartitionTraceHelper : IBatchWorkerTraceHelper
    {
        readonly ILogger logger;
        readonly string account;
        readonly string taskHub;
        readonly int partitionId;
        readonly LogLevel logLevelLimit;

        public PartitionTraceHelper(ILogger logger, LogLevel logLevelLimit, string storageAccountName, string taskHubName, uint partitionId)
        {
            this.logger = logger;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.partitionId = (int)partitionId;
            this.logLevelLimit = logLevelLimit;
        }

        public void TracePartitionProgress(string transition, ref double lastTransition, double time, string details)
        {
            double latencyMs = time - lastTransition;
            lastTransition = time;

            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} {transition} partition after {latencyMs:F2}ms {details}", this.partitionId, transition, latencyMs, details);
                 
                EtwSource.Log.PartitionProgress(this.account, this.taskHub, this.partitionId, transition, latencyMs, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void TracePartitionLoad(PartitionLoadInfo info)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Publishing LoadInfo WorkItems={workItems} Activities={activities} Timers={timers} Requests={requests} Outbox={outbox} Instances={instances} Wakeup={wakeup} WorkerId={workerId} LatencyTrend={latencyTrend} MissRate={missRate} InputQueuePosition={inputQueuePosition} CommitLogPosition={commitLogPosition}",
                    this.partitionId, info.WorkItems, info.Activities, info.Timers, info.Requests, info.Outbox, info.Instances, info.Wakeup, info.WorkerId, info.LatencyTrend, info.MissRate, info.InputQueuePosition, info.CommitLogPosition);

                EtwSource.Log.PartitionLoadPublished(this.account, this.taskHub, this.partitionId, info.WorkItems, info.Activities, info.Timers, info.Requests, info.Outbox, info.Instances, info.Wakeup?.ToString("o") ?? "", info.WorkerId, info.LatencyTrend, info.MissRate, info.InputQueuePosition, info.CommitLogPosition, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceBatchWorkerProgress(string worker, int batchSize, double elapsedMilliseconds, int? nextBatch)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger.LogDebug("Part{partition:D2} {worker} completed batch: batchSize={batchSize} elapsedMilliseconds={elapsedMilliseconds:F2} nextBatch={nextBatch}",
                    this.partitionId, worker, batchSize, elapsedMilliseconds, nextBatch.ToString() ?? "");
       
                EtwSource.Log.BatchWorkerProgress(this.account, this.taskHub, this.partitionId.ToString(), worker, batchSize, elapsedMilliseconds, nextBatch.ToString() ?? "", TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }
    }
}
