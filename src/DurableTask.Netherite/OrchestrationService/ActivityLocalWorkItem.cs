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

    class ActivityLocalWorkItem : ActivityWorkItem
    {
        public Partition Partition { get; set; }

        public long ActivityId { get; set; }

        public ActivityLocalWorkItem(Partition partition, long activityId, TaskMessage message, string originWorkItem)
            : base(partition.PartitionId, 
                   message, 
                   originWorkItem, 
                   ActivitiesState.GetWorkItemId(partition.PartitionId, activityId))
        {
            this.Partition = partition;
            this.ActivityId = activityId;
        }

        public override bool IsRemote => false;

        public override void HandleResultAsync(TaskMessage response, int reportedLoad, double latencyMs)
        {
            var workItemTraceHelper = this.Partition.WorkItemTraceHelper;

            var activityCompletedEvent = new LocalActivityCompleted()
            {
                PartitionId = this.Partition.PartitionId,
                ActivityId = this.ActivityId,
                OriginPartitionId = this.PartitionId,
                ReportedLoad = reportedLoad,
                Timestamp = DateTime.UtcNow,
                Response = response,
            };

            if (this.Partition.ErrorHandler.IsTerminated)
            {
                // we get here if the partition was terminated. The work is thrown away. 
                // It's unavoidable by design, but let's at least create a warning.
                workItemTraceHelper.TraceWorkItemDiscarded(
                    this.Partition.PartitionId,
                    WorkItemTraceHelper.WorkItemType.Activity,
                    this.Id,
                    this.TaskMessage.OrchestrationInstance.InstanceId,
                    "",
                    "partition was terminated"
                   );

                return;
            }

            workItemTraceHelper.TraceWorkItemCompleted(
                this.Partition.PartitionId,
                WorkItemTraceHelper.WorkItemType.Activity,
                this.Id,
                this.TaskMessage.OrchestrationInstance.InstanceId,
                WorkItemTraceHelper.ActivityStatus.Completed,
                latencyMs,
                1);

            try
            {
                this.Partition.SubmitInternalEvent(activityCompletedEvent);
                workItemTraceHelper.TraceTaskMessageSent(this.Partition.PartitionId, activityCompletedEvent.Response, this.WorkItemId, null, null);
            }
            catch (OperationCanceledException e)
            {
                // we get here if the partition was terminated. The work is thrown away. 
                // It's unavoidable by design, but let's at least create a warning.
                this.Partition.ErrorHandler.HandleError(
                    nameof(IOrchestrationService.CompleteTaskActivityWorkItemAsync),
                    "Canceling already-completed activity work item because of partition termination",
                    e,
                    false,
                    true);
            }
        }     
    }
}
