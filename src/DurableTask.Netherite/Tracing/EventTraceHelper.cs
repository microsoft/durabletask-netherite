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
        readonly LogLevel logLevelLimit;
        readonly string account;
        readonly string taskHub;
        readonly int partitionId;
        readonly EtwSource etw;

        public EventTraceHelper(ILoggerFactory loggerFactory, LogLevel logLevelLimit, Partition partition)
        {
            this.logger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.Events");
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
        }

        public void TraceInstanceUpdate(
            string workItemId, 
            string instanceId, 
            string executionId, 
            int totalEventCount, 
            List<HistoryEvent> newEvents, 
            int episode)
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

        public void TraceEventProcessingWarning(string details)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                (long commitLogPosition, string eventId) = EventTraceContext.Current;
                if (this.logger.IsEnabled(LogLevel.Warning))
                {
                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogWarning("Part{partition:D2}{prefix} {details}", this.partitionId, prefix, details);
                }
                this.etw?.PartitionEventWarning(this.account, this.taskHub, this.partitionId, commitLogPosition, eventId ?? "", details, TraceUtils.ExtensionVersion);
            }
        }
    }
}