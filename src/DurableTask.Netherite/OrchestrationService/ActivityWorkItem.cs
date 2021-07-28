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

    abstract class ActivityWorkItem : TaskActivityWorkItem
    {
        // the partition for the orchestration that issued this activity
        public uint PartitionId { get; }

        public string OriginWorkItem { get; }

        public double StartedAt { get; set; }

        protected ActivityWorkItem(uint partitionId, TaskMessage message, string originWorkItem, string workItemId)
        {
            this.PartitionId = partitionId;
            this.OriginWorkItem = originWorkItem;
            this.Id = workItemId;
            this.LockedUntilUtc = DateTime.MaxValue; // this backend does not require workitem lock renewals
            this.TaskMessage = message;
        }

        public abstract bool IsRemote { get; }

        public string WorkItemId => this.Id;

        public abstract void HandleResultAsync(TaskMessage response, int reportedLoad, double latencyMs);
    }
}
