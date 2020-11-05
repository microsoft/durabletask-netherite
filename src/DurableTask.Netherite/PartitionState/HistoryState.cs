// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;

    [DataContract]
    class HistoryState : TrackedObject
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public string ExecutionId { get; set; }

        [DataMember]
        public List<HistoryEvent> History { get; set; }

        [DataMember]
        public int Episode { get; set; }

        /// <summary>
        /// We cache this so we can resume the execution at the execution cursor.
        /// </summary>
        [IgnoreDataMember]
        public OrchestrationWorkItem CachedOrchestrationWorkItem { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.History, this.InstanceId);

        public override string ToString()
        {
            return $"History InstanceId={this.InstanceId} ExecutionId={this.ExecutionId} Events={this.History.Count}";
        }

        void DeleteHistory()
        {
            this.History = null;
            this.Episode = 0;
            this.ExecutionId = null;
            this.CachedOrchestrationWorkItem = null;
        }

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            // can add events to the history, or replace it with a new history

            // update the stored history
            if (this.History == null || evt.State.OrchestrationInstance.ExecutionId != this.ExecutionId)
            {
                this.History = new List<HistoryEvent>();
                this.Episode = 0;
                this.ExecutionId = evt.State.OrchestrationInstance.ExecutionId;
            }

            this.Partition.Assert(!string.IsNullOrEmpty(this.InstanceId) || string.IsNullOrEmpty(this.ExecutionId));

            // add all the new events to the history, and update episode number
            if (evt.NewEvents != null)
            {
                for (int i = 0; i < evt.NewEvents.Count; i++)
                {
                    var historyEvent = evt.NewEvents[i];
                    if (historyEvent.EventType == EventType.OrchestratorStarted)
                    {
                        this.Episode++;
                    }
                    this.History.Add(evt.NewEvents[i]);
                }
            }

            if (!effects.IsReplaying)
            {
                this.Partition.EventTraceHelper.TraceInstanceUpdate(
                    evt.WorkItemId,
                    evt.State.OrchestrationInstance.InstanceId,
                    evt.State.OrchestrationInstance.ExecutionId,
                    this.History.Count,
                    evt.NewEvents, this.Episode);

                // if present, we keep the work item so we can reuse the execution cursor
                this.CachedOrchestrationWorkItem = evt.WorkItemForReuse;

                if (this.CachedOrchestrationWorkItem != null 
                    && this.CachedOrchestrationWorkItem.OrchestrationRuntimeState?.OrchestrationInstance?.ExecutionId != evt.State.OrchestrationInstance.ExecutionId)
                {
                    effects.Partition.EventTraceHelper.TraceEventProcessingWarning($"Dropping bad workitem cache instance={this.InstanceId} expected_executionid={evt.State.OrchestrationInstance.ExecutionId} actual_executionid={this.CachedOrchestrationWorkItem.OrchestrationRuntimeState?.OrchestrationInstance?.ExecutionId}");
                    this.CachedOrchestrationWorkItem = null;
                }
            }
        }

        public void Process(DeletionRequestReceived deletionRequestReceived, EffectTracker effects)
        {
            this.DeleteHistory();
        }

        public void Process(PurgeBatchIssued purgeBatchIssued, EffectTracker effects)
        {
            this.DeleteHistory();
        }
    }
}