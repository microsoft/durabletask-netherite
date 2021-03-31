﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Diagnostics.Tracing;

    /// <summary>
    /// ETW Event Provider for the DurableTask.Netherite provider extension.
    /// </summary>
    /// <remarks>
    /// The ETW Provider ID for this event source is {25f9ede7-9f01-5a75-dba7-9c0ba798ac6c}.
    /// We list all events from the various layers (transport, storage) in this single file; however,
    /// we do have separate helper classes for each component.
    /// </remarks>
    [EventSource(Name = "DurableTask-Netherite")]
    class EtwSource : EventSource
    {
        /// <summary>
        /// Singleton instance used for writing events.
        /// </summary>
        public static readonly EtwSource Log = new EtwSource();

        // we should always check if verbose is enabled before doing extensive string formatting for a verbose event
        public bool IsVerboseEnabled => this.IsEnabled(EventLevel.Verbose, EventKeywords.None);

        // ----- orchestration service lifecycle

        // we are grouping all events by this instance of NetheriteOrchestrationService using a single activity id
        // and since there is only one of these per machine, we can save its id in this static field.
        static Guid serviceInstanceId;

        [Event(200, Level = EventLevel.Informational, Opcode = EventOpcode.Start, Version = 1)]
        public void OrchestrationServiceCreated(Guid OrchestrationServiceInstanceId, string Account, string TaskHub, string WorkerName, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(OrchestrationServiceInstanceId);
            this.WriteEvent(200, OrchestrationServiceInstanceId, Account, TaskHub, WorkerName, AppName, ExtensionVersion);
            EtwSource.serviceInstanceId = OrchestrationServiceInstanceId;
        }

        [Event(201, Level = EventLevel.Informational, Opcode = EventOpcode.Stop, Version = 1)]
        public void OrchestrationServiceStopped(Guid OrchestrationServiceInstanceId, string Account, string TaskHub, string WorkerName, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(OrchestrationServiceInstanceId);
            this.WriteEvent(201, OrchestrationServiceInstanceId, Account, TaskHub, WorkerName, AppName, ExtensionVersion);
        }

        [Event(202, Level = EventLevel.Error, Version = 1)]
        public void OrchestrationServiceError(string Account, string Message, string Details, string TaskHub, string WorkerName, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(202, Account, Message, Details, TaskHub, WorkerName, AppName, ExtensionVersion);
        }

        // ----- partition and client lifecycles

        [Event(210, Level = EventLevel.Informational, Version = 1)]
        public void PartitionProgress(string Account, string TaskHub, int PartitionId, string Transition, double LatencyMs, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(210, Account, TaskHub, PartitionId, Transition, LatencyMs, Details, AppName, ExtensionVersion);
        }

        [Event(211, Level = EventLevel.Warning, Version = 1)]
        public void PartitionWarning(string Account, string TaskHub, int PartitionId, string Context, bool TerminatesPartition, string Message, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(211, Account, TaskHub, PartitionId, Context, TerminatesPartition, Message, Details, AppName, ExtensionVersion);
        }

        [Event(212, Level = EventLevel.Error, Version = 1)]
        public void PartitionError(string Account, string TaskHub, int PartitionId, string Context, bool TerminatesPartition, string Message, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(212, Account, TaskHub, PartitionId, Context, TerminatesPartition, Message, Details, AppName, ExtensionVersion);
        }

        [Event(213, Level = EventLevel.Informational, Version = 1)]
        public void ClientProgress(string Account, string TaskHub, Guid ClientId, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(213, Account, TaskHub, ClientId, Details, AppName, ExtensionVersion);
        }

        [Event(214, Level = EventLevel.Warning, Version = 1)]
        public void ClientWarning(string Account, string TaskHub, Guid ClientId, string Context, string Message, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(214, Account, TaskHub, ClientId, Context, Message, Details, AppName, ExtensionVersion);
        }

        [Event(215, Level = EventLevel.Error, Version = 1)]
        public void ClientError(string Account, string TaskHub, Guid ClientId, string Context, string Message, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(215, Account, TaskHub, ClientId, Context, Message, Details, AppName, ExtensionVersion);
        }

        [Event(216, Level = EventLevel.Verbose, Version = 1)]
        public void ClientTimerProgress(string Account, string TaskHub, Guid ClientId, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(216, Account, TaskHub, ClientId, Details, AppName, ExtensionVersion);
        }

        [Event(217, Level = EventLevel.Warning, Version = 1)]
        public void ClientRequestTimeout(string Account, string TaskHub, Guid ClientId, string EventId, int PartitionId, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(217, Account, TaskHub, ClientId, EventId, PartitionId, AppName, ExtensionVersion);
        }

        // ----- specific events relating to DurableTask concepts (TaskMessage, OrchestrationWorkItem, Instance)

        [Event(220, Level = EventLevel.Verbose, Version = 1)]
        public void TaskMessageReceived(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, string EventType, int TaskEventId, string InstanceId, string ExecutionId, string QueuePosition, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(220, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, EventType, TaskEventId, InstanceId, ExecutionId, QueuePosition, AppName, ExtensionVersion);
        }

        [Event(221, Level = EventLevel.Verbose, Version = 1)]
        public void TaskMessageSent(string Account, string TaskHub, int PartitionId, string MessageId, string EventType, int TaskEventId, string InstanceId, string ExecutionId, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(221, Account, TaskHub, PartitionId, MessageId, EventType, TaskEventId, InstanceId, ExecutionId, AppName, ExtensionVersion);
        }

        [Event(222, Level = EventLevel.Warning, Version = 1)]
        public void TaskMessageDiscarded(string Account, string TaskHub, int PartitionId, string MessageId, string Reason, string EventType, int TaskEventId, string InstanceId, string ExecutionId, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(222, Account, TaskHub, PartitionId, MessageId, Reason, EventType, TaskEventId, InstanceId, ExecutionId, AppName, ExtensionVersion);
        }

        [Event(223, Level = EventLevel.Verbose, Version = 1)]
        public void WorkItemQueued(string Account, string TaskHub, int PartitionId, string WorkItemType, string WorkItemId, string InstanceId, string ExecutionType, string ConsumedMessageIds, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(223, Account, TaskHub, PartitionId, WorkItemType, WorkItemId, InstanceId, ExecutionType, ConsumedMessageIds, AppName, ExtensionVersion);
        }

        [Event(224, Level = EventLevel.Informational, Version = 1)]
        public void WorkItemStarted(string Account, string TaskHub, int PartitionId, string WorkItemType, string WorkItemId, string InstanceId, string ExecutionType, string ConsumedMessageIds, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(224, Account, TaskHub, PartitionId, WorkItemType, WorkItemId, InstanceId, ExecutionType, ConsumedMessageIds, AppName, ExtensionVersion);
        }

        [Event(225, Level = EventLevel.Informational, Version = 1)]
        public void WorkItemCompleted(string Account, string TaskHub, int PartitionId, string WorkItemType, string WorkItemId, string InstanceId, string Status, double LatencyMs, string ProducedMessageIds, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(225, Account, TaskHub, PartitionId, WorkItemType, WorkItemId, InstanceId, Status, LatencyMs, ProducedMessageIds, AppName, ExtensionVersion);
        }

        [Event(226, Level = EventLevel.Warning, Version = 1)]
        public void WorkItemDiscarded(string Account, string TaskHub, int PartitionId, string WorkItemType, string WorkItemId, string InstanceId, string Reason, string ReplacedBy, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(226, Account, TaskHub, PartitionId, WorkItemType, WorkItemId, InstanceId, Reason, ReplacedBy, AppName, ExtensionVersion);
        }

        [Event(227, Level = EventLevel.Verbose, Version = 1)]
        public void InstanceUpdated(string Account, string TaskHub, int PartitionId, string InstanceId, string ExecutionId, string WorkItemId, int NewEventCount, int TotalEventCount, string NewEvents, string EventType, int Episode, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(227, Account, TaskHub, PartitionId, InstanceId, ExecutionId, WorkItemId, NewEventCount, TotalEventCount, NewEvents, EventType, Episode, AppName, ExtensionVersion);
        }

        [Event(228, Level = EventLevel.Verbose, Version = 1)]
        public void InstanceStatusFetched(string Account, string TaskHub, int PartitionId, string InstanceId, string ExecutionId, string status, string EventId, double LatencyMs, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(228, Account, TaskHub, PartitionId, InstanceId, ExecutionId, status, EventId, LatencyMs, AppName, ExtensionVersion);
        }

        [Event(229, Level = EventLevel.Verbose, Version = 1)]
        public void InstanceHistoryFetched(string Account, string TaskHub, int PartitionId, string InstanceId, string ExecutionId, int EventCount, int Episode, string EventId, double LatencyMs, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(229, Account, TaskHub, PartitionId, InstanceId, ExecutionId, EventCount, Episode, EventId, LatencyMs, AppName, ExtensionVersion);
        }

        // ----- general event processing and statistics

        [Event(240, Level = EventLevel.Informational, Version = 1)]
        public void PartitionEventProcessed(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string Category, string EventId, string EventInfo, long NextCommitLogPosition, long NextInputQueuePosition, double QueueLatencyMs, double FetchLatencyMs, double LatencyMs, bool IsReplaying, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(240, Account, TaskHub, PartitionId, CommitLogPosition, Category, EventId, EventInfo, NextCommitLogPosition, NextInputQueuePosition, QueueLatencyMs, FetchLatencyMs, LatencyMs, IsReplaying, AppName, ExtensionVersion);
        }

        [Event(241, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionEventDetail(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string EventId, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(241, Account, TaskHub, PartitionId, CommitLogPosition, EventId, Details, AppName, ExtensionVersion);
        }

        [Event(242, Level = EventLevel.Warning, Version = 1)]
        public void PartitionEventWarning(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string EventId, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(242, Account, TaskHub, PartitionId, CommitLogPosition, EventId, Details, AppName, ExtensionVersion);
        }

        [Event(243, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventReceived(string Account, string TaskHub, Guid ClientId, string EventId, string EventInfo, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(243, Account, TaskHub, ClientId, EventId, EventInfo, AppName, ExtensionVersion);
        }

        [Event(244, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventSent(string Account, string TaskHub, Guid ClientId, string EventId, string EventInfo, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(244, Account, TaskHub, ClientId, EventId, EventInfo, AppName, ExtensionVersion);
        }

        [Event(245, Level = EventLevel.Warning, Version = 1)]
        public void PartitionOffloadDecision(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string EventId, int ReportedLocalLoad, int Pending, int Backlog, int Remotes, string ReportedRemoteLoad, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(245, Account, TaskHub, PartitionId, CommitLogPosition, EventId, ReportedLocalLoad, Pending, Backlog, Remotes, ReportedRemoteLoad, AppName, ExtensionVersion);
        }

        [Event(246, Level = EventLevel.Informational, Version = 1)]
        public void PartitionLoadPublished(string Account, string TaskHub, int PartitionId, int WorkItems, int Activities, int Timers, int Requests, int Outbox, string NextTimer, string WorkerId, string LatencyTrend, double MissRate, long InputQueuePosition, long CommitLogPosition, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(246, Account, TaskHub, PartitionId, WorkItems, Activities, Timers, Requests, Outbox, NextTimer, WorkerId, LatencyTrend, MissRate, InputQueuePosition, CommitLogPosition, AppName, ExtensionVersion);
        }

        [Event(247, Level = EventLevel.Verbose, Version = 1)]
        public void BatchWorkerProgress(string Account, string TaskHub, int PartitionId, string Worker, int BatchSize, double Latency, string NextBatch, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(247, Account, TaskHub, PartitionId, Worker, BatchSize, Latency, NextBatch, AppName, ExtensionVersion);
        }

        // ----- Faster Storage

        [Event(250, Level = EventLevel.Informational, Version = 1)]
        public void FasterStoreCreated(string Account, string TaskHub, int PartitionId, long InputQueuePosition, long LatencyMs, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(250, Account, TaskHub, PartitionId, InputQueuePosition, LatencyMs, AppName, ExtensionVersion);
        }

        [Event(251, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointStarted(string Account, string TaskHub, int PartitionId, Guid CheckpointId, string Reason, string StoreStats, long CommitLogPosition, long InputQueuePosition, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(251, Account, TaskHub, PartitionId, CheckpointId, Reason, StoreStats, CommitLogPosition, InputQueuePosition, AppName, ExtensionVersion);
        }

        [Event(252, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointPersisted(string Account, string TaskHub, int PartitionId, Guid CheckpointId, string Reason, long CommitLogPosition, long InputQueuePosition, long LatencyMs, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(252, Account, TaskHub, PartitionId, CheckpointId, Reason, CommitLogPosition, InputQueuePosition, LatencyMs, AppName, ExtensionVersion);
        }

        [Event(253, Level = EventLevel.Verbose, Version = 1)]
        public void FasterLogPersisted(string Account, string TaskHub, int PartitionId, long CommitLogPosition, long NumberEvents, long SizeInBytes, long LatencyMs, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(253, Account, TaskHub, PartitionId, CommitLogPosition, NumberEvents, SizeInBytes, LatencyMs, AppName, ExtensionVersion);
        }

        [Event(254, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointLoaded(string Account, string TaskHub, int PartitionId, long CommitLogPosition, long InputQueuePosition, string StoreStats, long LatencyMs, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(254, Account, TaskHub, PartitionId, CommitLogPosition, InputQueuePosition, StoreStats, LatencyMs, AppName, ExtensionVersion);
        }

        [Event(255, Level = EventLevel.Informational, Version = 1)]
        public void FasterLogReplayed(string Account, string TaskHub, int PartitionId, long CommitLogPosition, long InputQueuePosition, long NumberEvents, long SizeInBytes, string StoreStats, long LatencyMs, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(255, Account, TaskHub, PartitionId, CommitLogPosition, InputQueuePosition, NumberEvents, SizeInBytes, StoreStats, LatencyMs, AppName, ExtensionVersion);
        }

        [Event(256, Level = EventLevel.Error, Version = 1)]
        public void FasterStorageError(string Account, string TaskHub, int PartitionId, string Context, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(256, Account, TaskHub, PartitionId, Context, Details, AppName, ExtensionVersion);
        }

        [Event(259, Level = EventLevel.Verbose, Version = 1)]
        public void FasterProgress(string Account, string TaskHub, int PartitionId, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(259, Account, TaskHub, PartitionId, Details, AppName, ExtensionVersion);
        }

        [Event(260, Level = EventLevel.Informational, Version = 1)]
        public void FasterLeaseAcquired(string Account, string TaskHub, int PartitionId, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(260, Account, TaskHub, PartitionId, AppName, ExtensionVersion);
        }

        [Event(261, Level = EventLevel.Verbose, Version = 1)]
        public void FasterLeaseRenewed(string Account, string TaskHub, int PartitionId, double ElapsedSeconds, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(261, Account, TaskHub, PartitionId, ElapsedSeconds, AppName, ExtensionVersion);
        }

        [Event(262, Level = EventLevel.Informational, Version = 1)]
        public void FasterLeaseReleased(string Account, string TaskHub, int PartitionId, double ElapsedSeconds, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(262, Account, TaskHub, PartitionId, ElapsedSeconds, AppName, ExtensionVersion);
        }

        [Event(263, Level = EventLevel.Warning, Version = 1)]
        public void FasterLeaseLost(string Account, string TaskHub, int PartitionId, string Details, double ElapsedSeconds, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(263, Account, TaskHub, PartitionId, Details, ElapsedSeconds, AppName, ExtensionVersion);
        }

        [Event(264, Level = EventLevel.Verbose, Version = 1)]
        public void FasterLeaseProgress(string Account, string TaskHub, int PartitionId, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(264, Account, TaskHub, PartitionId, Details, AppName, ExtensionVersion);
        }

        [Event(265, Level = EventLevel.Verbose, Version = 1)]
        public void FasterStorageProgress(string Account, string TaskHub, int PartitionId, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(265, Account, TaskHub, PartitionId, Details, AppName, ExtensionVersion);
        }

        [Event(266, Level = EventLevel.Verbose, Version = 1)]
        public void FasterAzureStorageAccessCompleted(string Account, string TaskHub, int PartitionId, string Intent, long Size, string Operation, string Target, double Latency, int Attempt, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(266, Account, TaskHub, PartitionId, Intent, Size, Operation, Target, Latency, Attempt, AppName, ExtensionVersion);
        }

        [Event(267, Level = EventLevel.Warning, Version = 1)]
        public void FasterPerfWarning(string Account, string TaskHub, int PartitionId, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(267, Account, TaskHub, PartitionId, Details, AppName, ExtensionVersion);
        }

        // ----- EventHubs Transport

        [Event(270, Level = EventLevel.Informational, Version = 1)]
        public void EventHubsInformation(string Account, string TaskHub, string EventHubsNamespace, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(270, Account, TaskHub, EventHubsNamespace, Details, AppName, ExtensionVersion);
        }

        [Event(271, Level = EventLevel.Warning, Version = 1)]
        public void EventHubsWarning(string Account, string TaskHub, string EventHubsNamespace, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(271, Account, TaskHub, EventHubsNamespace, Details, AppName, ExtensionVersion);
        }

        [Event(272, Level = EventLevel.Error, Version = 1)]
        public void EventHubsError(string Account, string TaskHub, string EventHubsNamespace, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(272, Account, TaskHub, EventHubsNamespace, Details, AppName, ExtensionVersion);
        }

        [Event(273, Level = EventLevel.Verbose, Version = 1)]
        public void EventHubsDebug(string Account, string TaskHub, string EventHubsNamespace, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(273, Account, TaskHub, EventHubsNamespace, Details, AppName, ExtensionVersion);
        }

        [Event(274, Level = EventLevel.Verbose, Version = 1)]
        public void EventHubsTrace(string Account, string TaskHub, string EventHubsNamespace, string Details, string AppName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(274, Account, TaskHub, EventHubsNamespace, Details, AppName, ExtensionVersion);
        }
    }
}
