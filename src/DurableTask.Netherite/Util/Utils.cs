// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using DurableTask.Core;
    using DurableTask.Core.History;

    /// <summary>
    /// Functionality for tracing event histories.
    /// </summary>
    static class TraceUtils
    {
        // DurableTask.Core has a public static variable that contains the app name
        public static readonly string AppName = DurableTask.Core.Common.Utils.AppName;

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

        public static IEnumerable<string> GetLines(this string str, bool removeEmptyLines = false)
        {
            using (var sr = new StringReader(str))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    if (removeEmptyLines && String.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }
                    yield return line;
                }
            }
        }

        public static bool IsImplicitDeletion(this OrchestrationRuntimeState runtimeState)
        {
            return runtimeState.Events.Count == 3
               && runtimeState.Events[0].EventType == EventType.OrchestratorStarted
               && runtimeState.Events[1] is ExecutionStartedEvent executionStartedEvent
               && executionStartedEvent.Input == null
               && DurableTask.Core.Common.Entities.IsEntityInstance(executionStartedEvent.OrchestrationInstance.InstanceId)
               && runtimeState.Events[2].EventType == EventType.OrchestratorCompleted;
        }
    }
}
