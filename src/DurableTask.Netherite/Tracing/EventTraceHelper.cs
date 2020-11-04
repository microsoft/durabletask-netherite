// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Dynamitey;
    using Dynamitey.DynamicObjects;
    using FASTER.core;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    class EventTraceHelper
    {
        readonly ILogger logger;
        readonly ILogger benchmarkLogger;
        readonly LogLevel logLevelLimit;
        readonly string account;
        readonly string taskHub;
        readonly int partitionId;
        readonly EtwSource etw;

        public EventTraceHelper(ILoggerFactory loggerFactory, LogLevel logLevelLimit, Partition partition)
        {
            this.logger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.Events");
            this.benchmarkLogger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.Benchmark");
            this.logLevelLimit = logLevelLimit;
            this.account = partition.StorageAccountName;
            this.taskHub = partition.Settings.HubName;
            this.partitionId = (int)partition.PartitionId;
            this.etw = EtwSource.Log.IsEnabled() ? EtwSource.Log : null;
        }

        public bool IsTracingAtMostDetailedLevel => this.logLevelLimit == LogLevel.Trace;

        public void TraceEventProcessed(long commitLogPosition, PartitionEvent evt, double startedTimestamp, double finishedTimestamp, bool replaying)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                long nextCommitLogPosition = ((evt as PartitionUpdateEvent)?.NextCommitLogPosition) ?? 0L;
                double queueLatencyMs = evt.IssuedTimestamp - evt.ReceivedTimestamp;
                double fetchLatencyMs = startedTimestamp - evt.IssuedTimestamp;
                double latencyMs = finishedTimestamp - startedTimestamp;

                if (this.logger.IsEnabled(LogLevel.Information))
                {
                    var details = string.Format($"{(replaying ? "Replayed" : "Processed")} {(evt.NextInputQueuePosition > 0 ? "external" : "internal")} {(nextCommitLogPosition > 0 ? "update" : "read")} event");
                    this.logger.LogInformation("Part{partition:D2}.{commitLogPosition:D10} {details} {event} eventId={eventId} pos=({nextCommitLogPosition},{nextInputQueuePosition}) latency=({queueLatencyMs:F0}, {fetchLatencyMs:F0}, {latencyMs:F0})", this.partitionId, commitLogPosition, details, evt, evt.EventIdString, nextCommitLogPosition, evt.NextInputQueuePosition, queueLatencyMs, fetchLatencyMs, latencyMs);
                }

                this.etw?.PartitionEventProcessed(this.account, this.taskHub, this.partitionId, commitLogPosition, evt.EventIdString, evt.ToString(), nextCommitLogPosition, evt.NextInputQueuePosition, queueLatencyMs, fetchLatencyMs, latencyMs, replaying, TraceUtils.ExtensionVersion);
            }

            // TODO remove this for production, it is here for benchmarking only      
            //if (this.logLevelLimit <= LogLevel.Warning && !replaying)
            //{
            //    // This is here for measuring response times
            //    if (this.benchmarkLogger.IsEnabled(LogLevel.Warning))
            //    {
            //        string eventType;
            //        switch (evt)
            //        {
            //            case CreationRequestReceived creationRequestEvent:
            //                // Q: Is the receivedTimestamp the correct point to measure response time?
            //                var startTimestamp = creationRequestEvent.ReceivedTimestamp;
            //                var instanceId = creationRequestEvent.InstanceId;
            //                eventType = "CreationRequestReceived";
            //                this.benchmarkLogger.LogWarning("Part{partition:D2}.{commitLogPosition:D10} {eventType} for {instanceId} at {timestamp}", this.partitionId, commitLogPosition, eventType, instanceId, startTimestamp);
            //                break;

            //            // Old way of measuring orchestration completion time that involves noise due to client polling times
            //            //case StateRequestReceived readEvent:
            //            //    // Q: Is this how all our benchmarks end? It seems so.
            //            //    var endTimestamp = finishedTimestamp;
            //            //    var readTarget = readEvent.ReadTarget.InstanceId;
            //            //    eventType = "StateRequestReceived";
            //            //    this.logger.LogWarning("Part{partition:D2}.{commitLogPosition:D10} {eventType} for {instanceId} was processed at {timestamp}", this.partitionId, commitLogPosition, eventType, readTarget, endTimestamp);
            //            //    break;

            //            case BatchProcessed batchProcessedEvent:
            //                var state = batchProcessedEvent.State;
            //                if (state != null)
            //                {
            //                    var instanceStatus = state.OrchestrationStatus;
            //                    if (instanceStatus == OrchestrationStatus.Canceled ||
            //                        instanceStatus == OrchestrationStatus.Completed ||
            //                        instanceStatus == OrchestrationStatus.Failed ||
            //                        instanceStatus == OrchestrationStatus.Terminated)
            //                    {
            //                        var targetInstanceId = batchProcessedEvent.InstanceId;
            //                        var finalTimestamp = finishedTimestamp;
            //                        var creationTime = batchProcessedEvent.State.CreatedTime.Ticks;
            //                        var completionTime = batchProcessedEvent.State.CompletedTime.Ticks;
            //                        var elapsedTime = new TimeSpan(completionTime - creationTime);
            //                        eventType = "BatchProcessed";
            //                        this.benchmarkLogger.LogWarning("Part{partition:D2}.{commitLogPosition:D10} {status} {eventType} for {instanceId} was processed at {timestamp}", this.partitionId, commitLogPosition, instanceStatus, eventType, targetInstanceId, finalTimestamp);
            //                        //this.logger.LogWarning("Part{partition:D2}.{commitLogPosition:D10} {instanceId} with creation time: {creationTime} and completionTime: {completionTime} was {status} at {timestamp}", this.partitionId, commitLogPosition, targetInstanceId, creationTime, completionTime, instanceStatus, finalTimestamp);
            //                        this.benchmarkLogger.LogWarning("Part{partition:D2}.{commitLogPosition:D10} {instanceId} took {elapsedMilliseconds} ms from its creation to completion with status: {status} at {timestamp}", this.partitionId, commitLogPosition, targetInstanceId, elapsedTime.TotalMilliseconds, instanceStatus, finalTimestamp);
            //                    }

            //                }
            //                else
            //                {
            //                    // This could be `null` if the instance is not executable (e.g. when the messages where for an existing completed executionID).
            //                    var targetInstanceId = batchProcessedEvent.InstanceId;
            //                    var eventIdString = batchProcessedEvent.EventIdString;
            //                    eventType = "BatchProcessed";
            //                    this.benchmarkLogger.LogWarning("Part{partition:D2}.{commitLogPosition:D10} State was NULL in {eventType}:{eventId} for {instanceId} that was processed at {timestamp}", this.partitionId, commitLogPosition, eventType, eventIdString, targetInstanceId, finishedTimestamp);
            //                }
            //                break;
            //        }
            //    }
            //}
        }

        public void TraceTaskMessageReceived(TaskMessage message, string sessionPosition)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                (long commitLogPosition, string eventId) = EventTraceContext.Current;

                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogDebug("Part{partition:D2}{prefix} received TaskMessage eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId} messageId={messageId} sessionPosition={SessionPosition}",
                        this.partitionId, prefix, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId, eventId, sessionPosition);
                }

                this.etw?.TaskMessageReceived(this.account, this.taskHub, this.partitionId, commitLogPosition, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", eventId, sessionPosition, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceTaskMessageSent(TaskMessage message, string sentEventId)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogDebug("Part{partition:D2}{prefix} sent TaskMessage eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId} messageId={messageId}",
                        this.partitionId, prefix, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId, sentEventId);
                }

                this.etw?.TaskMessageSent(this.account, this.taskHub, this.partitionId, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", sentEventId, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceTaskMessageDiscarded(TaskMessage message, string reason, string workItemId)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                if (this.logger.IsEnabled(LogLevel.Warning))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogWarning("Part{partition:D2}{prefix} discarded TaskMessage reason={reason} eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId} workItemId={workItemId}",
                        this.partitionId, prefix, reason, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId, workItemId);
                }

                this.etw?.TaskMessageDiscarded(this.account, this.taskHub, this.partitionId, reason, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", workItemId, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceOrchestrationWorkItemQueued(OrchestrationWorkItem workItem)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogDebug("Part{partition:D2}{prefix} Queued {executionType} OrchestrationWorkItem {workItemId} instanceId={instanceId} ",
                        this.partitionId, prefix, workItem.Type, workItem.MessageBatch.WorkItemId, workItem.InstanceId);
                }

                this.etw?.OrchestrationWorkItemQueued(this.account, this.taskHub, this.partitionId, workItem.Type.ToString(), workItem.InstanceId, workItem.MessageBatch.WorkItemId, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceOrchestrationWorkItemDiscarded(BatchProcessed evt)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                if (this.logger.IsEnabled(LogLevel.Warning))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogWarning("Part{partition:D2}{prefix} Discarded OrchestrationWorkItem workItemId={workItemId} instanceId={instanceId}",
                        this.partitionId, prefix, evt.WorkItemId, evt.InstanceId);
                }

                this.etw?.OrchestrationWorkItemDiscarded(this.account, this.taskHub, this.partitionId, evt.InstanceId, evt.WorkItemId, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceInstanceUpdate(string workItemId, string instanceId, string executionId, int totalEventCount, List<HistoryEvent> newEvents, int episode)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                string eventNames = string.Empty;
                string eventType = string.Empty;
                int numNewEvents = 0;

                if (newEvents != null)
                {
                    HistoryEvent orchestratorEvent = null;
                    var eventNamesBuilder = new StringBuilder();
                    numNewEvents = newEvents.Count;

                    if (numNewEvents > 20)
                    {
                        eventNamesBuilder.Append("...,");
                    }

                    for (int i = 0; i < numNewEvents; i++)
                    {
                        var historyEvent = newEvents[i];
                        switch (historyEvent.EventType)
                        {
                            case EventType.ExecutionStarted:
                            case EventType.ExecutionCompleted:
                            case EventType.ExecutionTerminated:
                            case EventType.ContinueAsNew:
                                orchestratorEvent = historyEvent;
                                break;
                            default:
                                break;
                        }
                        if (i >= newEvents.Count - 20)
                        {
                            eventNamesBuilder.Append(historyEvent.EventType.ToString());
                            eventNamesBuilder.Append(",");
                        }
                    }
                    eventNames = eventNamesBuilder.ToString(0, eventNamesBuilder.Length - 1); // remove trailing comma
                    if (orchestratorEvent != null)
                    {
                        eventType = orchestratorEvent.EventType.ToString();
                    }
                }

                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogDebug("Part{partition:D2}{prefix} Updated instance instanceId={instanceId} executionId={executionId} workItemId={workItemId} numNewEvents={numNewEvents} totalEventCount={totalEventCount} eventNames={eventNames} eventType={eventType} episode={episode}",
                        this.partitionId, prefix, instanceId, executionId, workItemId, numNewEvents, totalEventCount, eventNames, eventType, episode);
                }

                this.etw?.InstanceUpdated(this.account, this.taskHub, this.partitionId, instanceId, executionId, workItemId, numNewEvents, totalEventCount, eventNames, eventType, episode, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceFetchedInstanceStatus(PartitionReadEvent evt, string instanceId, string executionId, string status, double latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogDebug("Part{partition:D2}{prefix} Fetched instance status instanceId={instanceId} executionId={executionId} status={status} eventId={eventId} latencyMs={latencyMs:F0}",
                        this.partitionId, prefix, instanceId, executionId, status, evt.EventIdString, latencyMs);
                }

                this.etw?.InstanceStatusFetched(this.account, this.taskHub, this.partitionId, instanceId, executionId, status, evt.EventIdString, latencyMs, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceFetchedInstanceHistory(PartitionReadEvent evt, string instanceId, string executionId, int eventCount, int episode, double latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogDebug("Part{partition:D2}{prefix} Fetched instance history instanceId={instanceId} executionId={executionId} eventCount={eventCount} episode={episode} eventId={eventId} latencyMs={latencyMs:F0}",
                        this.partitionId, prefix, instanceId, executionId, eventCount, episode, evt.EventIdString, latencyMs);
                }

                this.etw?.InstanceHistoryFetched(this.account, this.taskHub, this.partitionId, instanceId, executionId ?? string.Empty, eventCount, episode, evt.EventIdString, latencyMs, TraceUtils.ExtensionVersion);
            }
        }

        public void TracePartitionOffloadDecision(int reportedLocalLoad, int pending, int backlog, int remotes, string reportedRemotes)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                if (this.logLevelLimit <= LogLevel.Warning)
                {
                    (long commitLogPosition, string eventId) = EventTraceContext.Current;

                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogWarning("Part{partition:D2}{prefix} Offload decision reportedLocalLoad={reportedLocalLoad} pending={pending} backlog={backlog} remotes={remotes} reportedRemotes={reportedRemotes}",
                        this.partitionId, prefix, reportedLocalLoad, pending, backlog, remotes, reportedRemotes);

                    this.etw?.PartitionOffloadDecision(this.account, this.taskHub, this.partitionId, commitLogPosition, eventId, reportedLocalLoad, pending, backlog, remotes, reportedRemotes, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceEventProcessingStarted(long commitLogPosition, PartitionEvent evt, bool replaying)
        {
            if (this.logLevelLimit <= LogLevel.Trace)
            {
                long nextCommitLogPosition = ((evt as PartitionUpdateEvent)?.NextCommitLogPosition) ?? 0L;
                var details = string.Format($"{(replaying ? "Replaying" : "Processing")} {(evt.NextInputQueuePosition > 0 ? "external" : "internal")} {(nextCommitLogPosition > 0 ? "update" : "read")} event {evt} id={evt.EventIdString}");
                if (this.logger.IsEnabled(LogLevel.Trace))
                {
                    this.logger.LogTrace("Part{partition:D2}.{commitLogPosition:D10} {details}", this.partitionId, commitLogPosition, details);
                }
                this.etw?.PartitionEventDetail(this.account, this.taskHub, this.partitionId, commitLogPosition, evt.EventIdString ?? "", details, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceEventProcessingDetail(string details)
        {
            if (this.logLevelLimit <= LogLevel.Trace)
            {
                (long commitLogPosition, string eventId) = EventTraceContext.Current;
                if (this.logger.IsEnabled(LogLevel.Trace))
                {
                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogTrace("Part{partition:D2}{prefix} {details}", this.partitionId, prefix, details);
                }
                this.etw?.PartitionEventDetail(this.account, this.taskHub, this.partitionId, commitLogPosition, eventId ?? "", details, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceEventSentDetail(PartitionUpdateEvent evt)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                (long commitLogPosition, string eventId) = EventTraceContext.Current;
                double sentTimestamp = evt.SentTimestamp;
                double noSpeculationDelay = sentTimestamp - ((PartitionUpdateEvent)evt).ReadyToSendTimestamp;
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogDebug("Part{partition:D2}{prefix} Event sent with delay: {noSpeculationDelay} ms", this.partitionId, prefix, noSpeculationDelay);
                }
                this.etw?.PartitionEventDetail(this.account, this.taskHub, this.partitionId, commitLogPosition, eventId ?? "", $"Event sent with delay: {noSpeculationDelay} ms", TraceUtils.ExtensionVersion);
            }
        }

        public void TraceWarning(string details)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                (long commitLogPosition, string eventId) = EventTraceContext.Current;
                if (this.logger.IsEnabled(LogLevel.Warning))
                {
                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogWarning("Part{partition:D2}{prefix} {details}", this.partitionId, prefix, details);
                }

                // for now no ETW, TODO
            }
        }
    }
}