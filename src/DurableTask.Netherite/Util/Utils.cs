// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using DurableTask.Core;
    using DurableTask.Core.History;

    /// <summary>
    /// Functionality for tracing event histories.
    /// </summary>
    static class TraceUtils
    {
        public static readonly string ExtensionVersion = FileVersionInfo.GetVersionInfo(typeof(NetheriteOrchestrationService).Assembly.Location).FileVersion;

        public static int GetEpisodeNumber(OrchestrationRuntimeState runtimeState)
        {
            return GetEpisodeNumber(runtimeState.Events);
        }

        public static int GetEpisodeNumber(IEnumerable<HistoryEvent> historyEvents)
        {
            // DTFx core writes an "OrchestratorStarted" event at the start of each episode.
            return historyEvents.Count(e => e.EventType == EventType.OrchestratorStarted);
        }

        public static int GetTaskEventId(HistoryEvent historyEvent)
        {
            if (TryGetTaskScheduledId(historyEvent, out int taskScheduledId))
            {
                return taskScheduledId;
            }

            return historyEvent.EventId;
        }

        public static bool TryGetTaskScheduledId(HistoryEvent historyEvent, out int taskScheduledId)
        {
            switch (historyEvent.EventType)
            {
                case EventType.TaskCompleted:
                    taskScheduledId = ((TaskCompletedEvent)historyEvent).TaskScheduledId;
                    return true;
                case EventType.TaskFailed:
                    taskScheduledId = ((TaskFailedEvent)historyEvent).TaskScheduledId;
                    return true;
                case EventType.SubOrchestrationInstanceCompleted:
                    taskScheduledId = ((SubOrchestrationInstanceCompletedEvent)historyEvent).TaskScheduledId;
                    return true;
                case EventType.SubOrchestrationInstanceFailed:
                    taskScheduledId = ((SubOrchestrationInstanceFailedEvent)historyEvent).TaskScheduledId;
                    return true;
                case EventType.TimerFired:
                    taskScheduledId = ((TimerFiredEvent)historyEvent).TimerId;
                    return true;
                default:
                    taskScheduledId = -1;
                    return false;
            }
        }
    }
}
