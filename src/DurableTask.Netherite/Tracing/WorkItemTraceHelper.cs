// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Netherite.Tracing;
    using Microsoft.Extensions.Logging;

    class WorkItemTraceHelper
    {
        readonly ILogger logger;
        readonly LogLevel logLevelLimit;
        readonly string taskHub;
        readonly EtwSource etw;
        public string StorageAccountName { private get; set; } = string.Empty;

        public RempFormat.IListener RempTracer { get; set; }

        public static IEnumerable<Tracing.RempFormat.WorkitemGroup> GetRempGroups(NetheriteOrchestrationServiceSettings settings) =>
            new Tracing.RempFormat.WorkitemGroup[]
                {
                    new Tracing.RempFormat.WorkitemGroup() {  Name = "Client",        DegreeOfParallelism = null                                        },
                    new Tracing.RempFormat.WorkitemGroup() {  Name = "Orchestrator",  DegreeOfParallelism = settings.MaxConcurrentOrchestratorFunctions },
                    new Tracing.RempFormat.WorkitemGroup() {  Name = "Activity",      DegreeOfParallelism = settings.MaxConcurrentActivityFunctions     },
                };

        // must match order above
        public const int RempGroupClient = 0;
        public const int RempGroupOrchestration = 1;
        public const int RempGroupActivity = 2;

        public static string FormatMessageId(TaskMessage message, string workItem)
            => $"{workItem}M{message.SequenceNumber}";

        public static string FormatMessageIdList(IEnumerable<(TaskMessage message, string workItem)> messages)
        {
            // we include only up to 100 messages into a trace event.
            var sb = new StringBuilder();
            using var enumerator = messages.GetEnumerator();
            for (int i = 0; i < 100; i++)
            {
                if (enumerator.MoveNext())
                {
                    if (i > 0)
                    {
                        sb.Append(',');
                    }
                    sb.Append(FormatMessageId(enumerator.Current.message, enumerator.Current.workItem));
                }
                else
                {
                    return sb.ToString();
                }
            }
            sb.Append(",...");
            return sb.ToString();
        }

        public static string FormatEmptyMessageIdList() => string.Empty;

        public static string FormatClientWorkItemId(Guid clientId, long requestId)
           => $"{Client.GetShortId(clientId)}R{requestId}";

        public enum WorkItemType
        {
            None,
            Client,
            Activity,
            Orchestration
        }

        public enum ClientStatus
        {
            None,
            Create,
            Send,
        }

        public enum ActivityStatus
        {
            None,
            Completed
        }


        public WorkItemTraceHelper(ILoggerFactory loggerFactory, LogLevel logLevelLimit, string taskHub)
        {
            this.logger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.WorkItems");
            this.logLevelLimit = logLevelLimit;
            this.taskHub = taskHub;
            this.etw = EtwSource.Log.IsEnabled() ? EtwSource.Log : null;
        }

        public void TraceWorkItemQueued(uint partitionId, WorkItemType workItemType, string workItemId, string instanceId, string executionType, int eventCount, string consumedMessageIds)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("Part{partition:D2} queued {workItemType}WorkItem {workItemId} instanceId={instanceId} executionType={executionType} eventCount={eventCount} consumedMessageIds={consumedMessageIds}",
                        partitionId, workItemType, workItemId, instanceId, executionType, eventCount, consumedMessageIds);
                }

                this.etw?.WorkItemQueued(this.StorageAccountName, this.taskHub, (int)partitionId, workItemType.ToString(), workItemId, instanceId, executionType, eventCount, consumedMessageIds, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceWorkItemStarted(uint partitionId, WorkItemType workItemType, string workItemId, string instanceId, string executionType, string consumedMessageIds)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                if (this.logger.IsEnabled(LogLevel.Information))
                {
                    this.logger.LogDebug("Part{partition:D2} started {workItemType}WorkItem {workItemId} instanceId={instanceId} executionType={executionType} consumedMessageIds={consumedMessageIds}",
                        partitionId, workItemType, workItemId, instanceId, executionType, consumedMessageIds);
                }

                this.etw?.WorkItemStarted(this.StorageAccountName, this.taskHub, (int)partitionId, workItemType.ToString(), workItemId, instanceId, executionType, consumedMessageIds, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceWorkItemDiscarded(uint partitionId, WorkItemType workItemType, string workItemId, string instanceId, string replacedBy, string details)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                if (this.logger.IsEnabled(LogLevel.Warning))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogWarning("Part{partition:D2}{prefix} discarded {workItemType}WorkItem {workItemId} because {details}; instanceId={instanceId} replacedBy={replacedBy}",
                        partitionId, prefix, workItemType, workItemId, details, instanceId, replacedBy);
                }

                this.etw?.WorkItemDiscarded(this.StorageAccountName, this.taskHub, (int)partitionId, workItemType.ToString(), workItemId, instanceId, details, replacedBy ?? "", TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceWorkItemCompleted(uint partitionId, WorkItemType workItemType, string workItemId, string instanceId, object status, double latencyMs, long producedMessages)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                if (this.logger.IsEnabled(LogLevel.Information))
                {
                    this.logger.LogInformation("Part{partition:D2} completed {workItemType}WorkItem {workItemId} instanceId={instanceId} status={status} latencyMs={latencyMs:F2} producedMessages={producedMessages}",
                        partitionId, workItemType, workItemId, instanceId, status, latencyMs, producedMessages);
                }

                this.etw?.WorkItemCompleted(this.StorageAccountName, this.taskHub, (int)partitionId, workItemType.ToString(), workItemId, instanceId, status.ToString(), latencyMs, producedMessages, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public bool TraceTaskMessages => this.logLevelLimit <= LogLevel.Trace;

        public void TraceTaskMessageReceived(uint partitionId, TaskMessage message, string workItemId, string queuePosition)
        {
            if (this.logLevelLimit <= LogLevel.Trace)
            {
                (long commitLogPosition, string eventId) = EventTraceContext.Current;
                string messageId = FormatMessageId(message, workItemId);

                if (this.logger.IsEnabled(LogLevel.Trace))
                {
                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogTrace("Part{partition:D2}{prefix} received TaskMessage {messageId} eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId} queuePosition={QueuePosition}",
                        partitionId, prefix, messageId, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId, queuePosition);
                }

                this.etw?.TaskMessageReceived(this.StorageAccountName, this.taskHub, (int)partitionId, commitLogPosition, messageId, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", queuePosition, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceTaskMessageSent(uint partitionId, TaskMessage message, string workItemId, double? persistenceDelay, double? sendDelay)
        {
            if (this.logLevelLimit <= LogLevel.Trace)
            {
                string messageId = FormatMessageId(message, workItemId);
                string persistenceDelayMs = persistenceDelay.HasValue ? persistenceDelay.Value.ToString("F2") : string.Empty;
                string sendDelayMs = sendDelay.HasValue ? sendDelay.Value.ToString("F2") : string.Empty;

                if (this.logger.IsEnabled(LogLevel.Trace))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogTrace("Part{partition:D2}{prefix} sent TaskMessage {messageId} eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId} persistenceDelayMs={persistenceDelayMs} sendDelayMs={sendDelayMs}",
                        partitionId, prefix, messageId, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId, persistenceDelayMs, sendDelayMs);
                }

                this.etw?.TaskMessageSent(this.StorageAccountName, this.taskHub, (int)partitionId, messageId, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", persistenceDelayMs, sendDelayMs, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceTaskMessageDiscarded(uint partitionId, TaskMessage message, string workItemId, string details)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                string messageId = FormatMessageId(message, workItemId);

                if (this.logger.IsEnabled(LogLevel.Warning))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogWarning("Part{partition:D2}{prefix} discarded TaskMessage {messageId} because {details} eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId}",
                        partitionId, prefix, messageId, details, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId);
                }

                this.etw?.TaskMessageDiscarded(this.StorageAccountName, this.taskHub, (int)partitionId, messageId, details, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }
    }
}