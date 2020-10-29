//  ----------------------------------------------------------------------------------
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

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using DurableTask.Core;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Extensions.Logging;

    class PartitionTraceHelper
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

        public void TraceProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} {details}", this.partitionId, details);
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.PartitionProgress(this.account, this.taskHub, this.partitionId, details, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TracePartitionLoad(PartitionLoadInfo info)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Publishing LoadInfo WorkItems={workItems} Activities={activities} Timers={timers} Requests={requests} Outbox={outbox} Wakeup={wakeup} ActivityLatencyMs={activityLatencyMs} WorkItemLatencyMs={workItemLatencyMs} WorkerId={workerId} LatencyTrend={latencyTrend} MissRate={missRate} InputQueuePosition={inputQueuePosition} CommitLogPosition={commitLogPosition}",
                    this.partitionId, info.WorkItems, info.Activities, info.Timers, info.Requests, info.Outbox, info.Wakeup, info.ActivityLatencyMs, info.WorkItemLatencyMs, info.WorkerId, info.LatencyTrend, info.MissRate, info.InputQueuePosition, info.CommitLogPosition);

                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.PartitionLoadPublished(this.account, this.taskHub, this.partitionId, info.WorkItems, info.Activities, info.Timers, info.Requests, info.Outbox, info.Wakeup?.ToString("o") ?? "", info.ActivityLatencyMs, info.WorkItemLatencyMs, info.WorkerId, info.LatencyTrend, info.MissRate, info.InputQueuePosition, info.CommitLogPosition, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceWorkItemProgress(string workItemId, string instanceId, string format, params object[] args)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    object[] objarray = new object[3 + args.Length];
                    objarray[0] = this.partitionId;
                    objarray[1] = workItemId;
                    objarray[2] = instanceId;
                    Array.Copy(args, 0, objarray, 3, args.Length);
                    this.logger.LogDebug("Part{partition:D2} OrchestrationWorkItem workItemId={workItemId} instanceId={instanceId} " + format, objarray);
                }
            }
        }
    }
}