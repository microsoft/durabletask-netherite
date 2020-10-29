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
