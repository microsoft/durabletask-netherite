// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;

    class ActivityRemoteWorkItem : ActivityWorkItem
    {
        public Worker Worker { get; set; }

        public ActivityRemoteWorkItem(Worker worker, uint originPartition, TaskMessage message, string originWorkItem, long sequenceNumber)
            : base(originPartition, message, originWorkItem, worker.GetWorkItemId(sequenceNumber))
        {
            this.Worker = worker;
        }

        public override bool IsRemote => true;

        public override void HandleResultAsync(TaskMessage response, int reportedLoad, double latencyMs)
        {
            WorkItemTraceHelper workItemTraceHelper = this.Worker.WorkItemTraceHelper;

            var resultMessage = new WorkerResultReceived()
            {
                PartitionId = this.PartitionId,
                OriginWorkItemId = this.OriginWorkItem,
                OriginSequenceNumber = this.TaskMessage.SequenceNumber,
                Result = response,
            };

            workItemTraceHelper.TraceWorkItemCompleted(
                this.PartitionId,
                WorkItemTraceHelper.WorkItemType.Activity,
                this.Id,
                this.TaskMessage.OrchestrationInstance.InstanceId,
                WorkItemTraceHelper.ActivityStatus.Completed,
                latencyMs,
                1);

            try
            {
                this.Worker.BatchSender.Submit(resultMessage);
                workItemTraceHelper.TraceTaskMessageSent(this.PartitionId, response, this.WorkItemId, null, null);
            }
            catch (OperationCanceledException)
            {
                workItemTraceHelper.TraceWorkItemDiscarded(
                this.PartitionId,
                WorkItemTraceHelper.WorkItemType.Activity,
                this.Id,
                this.TaskMessage.OrchestrationInstance.InstanceId,
                "null",
                "Worker was terminated");
            }
        }
    }
}
