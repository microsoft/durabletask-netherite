// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.Extensions.Logging;

    class WorkItemTraceHelper
    {
        readonly ILogger logger;
        readonly LogLevel logLevelLimit;
        readonly string account;
        readonly string taskHub;
        readonly EtwSource etw;

        public static string FormatMessageId(TaskMessage message, string workItem)
            => $"{workItem}M{message.SequenceNumber}";

        public static string FormatMessageIdList(IEnumerable<(TaskMessage message, string workItem)> messages)
            => string.Join(",", messages.Select(entry => FormatMessageId(entry.message, entry.workItem)));

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


        public WorkItemTraceHelper(ILoggerFactory loggerFactory, LogLevel logLevelLimit, string account, string taskHub)
        {
            this.logger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.WorkItems");
            this.logLevelLimit = logLevelLimit;
            this.account = account;
            this.taskHub = taskHub;
            this.etw = EtwSource.Log.IsEnabled() ? EtwSource.Log : null;
        }

        public void TraceWorkItemQueued(uint partitionId, WorkItemType workItemType, string workItemId, string instanceId, string executionType, string consumedMessageIds)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("Part{partition:D2} queued {workItemType}WorkItem {workItemId} instanceId={instanceId} executionType={executionType} consumedMessageIds={consumedMessageIds}",
                        partitionId, workItemType, workItemId, instanceId, executionType, consumedMessageIds);
                }

                this.etw?.WorkItemQueued(this.account, this.taskHub, (int)partitionId, workItemType.ToString(), workItemId, instanceId, executionType, consumedMessageIds, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceWorkItemStarted(uint partitionId, WorkItemType workItemType, string workItemId, string instanceId, string executionType)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("Part{partition:D2} started {workItemType}WorkItem {workItemId} instanceId={instanceId} executionType={executionType}",
                        partitionId, workItemType, workItemId, instanceId, executionType);
                }

                this.etw?.WorkItemStarted(this.account, this.taskHub, (int)partitionId, workItemType.ToString(), workItemId, instanceId, executionType, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceWorkItemDiscarded(uint partitionId, WorkItemType workItemType, string workItemId, string instanceId)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                if (this.logger.IsEnabled(LogLevel.Warning))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogWarning("Part{partition:D2}{prefix} discarded {workItemType}WorkItem {workItemId} because instance was replaced; instanceId={instanceId} ",
                        partitionId, prefix, workItemType, workItemId, instanceId);
                }

                this.etw?.WorkItemDiscarded(this.account, this.taskHub, (int)partitionId, workItemType.ToString(), workItemId, instanceId, TraceUtils.ExtensionVersion);
            }
        }


        public void TraceWorkItemCompleted(uint partitionId, WorkItemType workItemType, string workItemId, string instanceId, object status, string producedMessageIds)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("Part{partition:D2} completed {workItemType}WorkItem {workItemId} instanceId={instanceId} status={status} producedMessageIds={producedMessageIds}",
                        partitionId, workItemType, workItemId, instanceId, status, producedMessageIds);
                }

                this.etw?.WorkItemCompleted(this.account, this.taskHub, (int)partitionId, workItemType.ToString(), workItemId, instanceId, status.ToString(), producedMessageIds, TraceUtils.ExtensionVersion);
            }
        }

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

                this.etw?.TaskMessageReceived(this.account, this.taskHub, (int)partitionId, commitLogPosition, messageId, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", queuePosition, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceTaskMessageSent(uint partitionId, TaskMessage message, string workItemId, string sentEventId)
        {
            if (this.logLevelLimit <= LogLevel.Trace)
            {
                string messageId = FormatMessageId(message, workItemId);

                if (this.logger.IsEnabled(LogLevel.Trace))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogTrace("Part{partition:D2}{prefix} sent TaskMessage {messageId} eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId}",
                        partitionId, prefix, messageId, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId);
                }

                this.etw?.TaskMessageSent(this.account, this.taskHub, (int)partitionId, messageId, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", TraceUtils.ExtensionVersion);
            }
        }

        public void TraceTaskMessageDiscarded(uint partitionId, TaskMessage message, string workItemId, string reason)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                string messageId = FormatMessageId(message, workItemId);

                if (this.logger.IsEnabled(LogLevel.Warning))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogWarning("Part{partition:D2}{prefix} discarded TaskMessage {messageId} reason={reason} eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId}",
                        partitionId, prefix, messageId, reason, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId);
                }

                this.etw?.TaskMessageDiscarded(this.account, this.taskHub, (int)partitionId, messageId, reason, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", TraceUtils.ExtensionVersion);
            }
        }
    }
}