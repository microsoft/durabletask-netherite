// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using DurableTask.Core;
    using DurableTask.Core.History;

    /// <summary>
    /// Functionality for estimating memory size.
    /// </summary>
    class SizeUtils
    {
        public static long GetEstimatedSize(OrchestrationInstance instance)
        {
            return instance == null ? 0 : 32 + 2 * (instance.InstanceId?.Length ?? 0 + instance.ExecutionId?.Length ?? 0);
        }

        public static long GetEstimatedSize(ParentInstance p)
        {
            return p == null ? 0 : 36 + 2 * (p.Name?.Length ?? 0 + p.Version?.Length ?? 0) + GetEstimatedSize(p.OrchestrationInstance);
        }

        public static long GetEstimatedSize(OrchestrationState state)
        {
            //DateTime CompletedTime;
            long sum = 0;
            if (state != null)
            {
                sum += 120;
                sum += 2 * ((state.Status?.Length ?? 0) + (state.Output?.Length ?? 0) + (state.Name?.Length ?? 0) + (state.Input?.Length ?? 0) + (state.Version?.Length ?? 0));
                sum += GetEstimatedSize(state.OrchestrationInstance) + GetEstimatedSize(state.ParentInstance);
            }
            return sum;
        }

        public static long GetEstimatedSize(HistoryEvent historyEvent)
        {
            long estimate = 5*8; // estimated size of base class
            void AddString(string s) => estimate += 8 + ((s == null) ? 0 : 2 * s.Length);
            switch (historyEvent)
            {
                case ContinueAsNewEvent continueAsNewEvent:
                    AddString(continueAsNewEvent.Result);
                    break;
                case EventRaisedEvent eventRaisedEvent:
                    AddString(eventRaisedEvent.Input);
                    AddString(eventRaisedEvent.Name);
                    break;
                case EventSentEvent eventSentEvent:
                    AddString(eventSentEvent.InstanceId);
                    AddString(eventSentEvent.Name);
                    AddString(eventSentEvent.Input);
                    break;
                case ExecutionCompletedEvent executionCompletedEvent:
                    AddString(executionCompletedEvent.Result);
                    estimate += 8;
                    break;
                case ExecutionStartedEvent executionStartedEvent:
                    AddString(executionStartedEvent.Input);
                    AddString(executionStartedEvent.Name);
                    AddString(executionStartedEvent.OrchestrationInstance.InstanceId);
                    AddString(executionStartedEvent.OrchestrationInstance.ExecutionId);
                    estimate += 8 + (executionStartedEvent.Tags == null ? 0 
                        : executionStartedEvent.Tags.Select(kvp => 20 + 2 * (kvp.Key.Length + kvp.Value.Length)).Sum());
                    AddString(executionStartedEvent.Version);
                    AddString(executionStartedEvent.ParentInstance?.OrchestrationInstance.InstanceId);
                    AddString(executionStartedEvent.ParentInstance?.OrchestrationInstance.ExecutionId);
                    estimate += 8;
                    break;
                case ExecutionTerminatedEvent executionTerminatedEvent:
                    AddString(executionTerminatedEvent.Input);
                    break;
                case GenericEvent genericEvent:
                    AddString(genericEvent.Data);
                    break;
                case OrchestratorCompletedEvent:
                    break;
                case OrchestratorStartedEvent:
                    break;
                case SubOrchestrationInstanceCompletedEvent subOrchestrationInstanceCompletedEvent:
                    estimate += 8;
                    AddString(subOrchestrationInstanceCompletedEvent.Result);
                    break;
                case SubOrchestrationInstanceCreatedEvent subOrchestrationInstanceCreatedEvent:
                    AddString(subOrchestrationInstanceCreatedEvent.Input);
                    AddString(subOrchestrationInstanceCreatedEvent.Input);
                    AddString(subOrchestrationInstanceCreatedEvent.Name);
                    AddString(subOrchestrationInstanceCreatedEvent.Version);
                    break;
                case SubOrchestrationInstanceFailedEvent subOrchestrationInstanceFailedEvent:
                    estimate += 8;
                    AddString(subOrchestrationInstanceFailedEvent.Reason);
                    AddString(subOrchestrationInstanceFailedEvent.Details);
                    break;
                case TaskCompletedEvent taskCompletedEvent:
                    estimate += 8;
                    AddString(taskCompletedEvent.Result);
                    break;
                case TaskFailedEvent taskFailedEvent:
                    estimate += 8;
                    AddString(taskFailedEvent.Reason);
                    AddString(taskFailedEvent.Details);
                    break;
                case TaskScheduledEvent taskScheduledEvent:
                    AddString(taskScheduledEvent.Input);
                    AddString(taskScheduledEvent.Name);
                    AddString(taskScheduledEvent.Version);
                    break;
                case TimerCreatedEvent timerCreatedEvent:
                    estimate += 8;
                    break;
                case TimerFiredEvent timerFiredEvent:
                    estimate += 16;
                    break;
            }
            return estimate;
        }

    }
}
