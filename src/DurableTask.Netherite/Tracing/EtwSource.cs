// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
        public void OrchestrationServiceCreated(Guid OrchestrationServiceInstanceId, string Account, string TaskHub, string WorkerName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(OrchestrationServiceInstanceId);
            this.WriteEvent(200, OrchestrationServiceInstanceId, Account, TaskHub, WorkerName, ExtensionVersion);
            EtwSource.serviceInstanceId = OrchestrationServiceInstanceId;
        }

        [Event(201, Level = EventLevel.Informational, Opcode = EventOpcode.Stop, Version = 1)]
        public void OrchestrationServiceStopped(Guid OrchestrationServiceInstanceId, string Account, string TaskHub, string WorkerName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(OrchestrationServiceInstanceId);
            this.WriteEvent(201, OrchestrationServiceInstanceId, Account, TaskHub, WorkerName, ExtensionVersion);
        }

        [Event(202, Level = EventLevel.Error, Version = 1)]
        public void OrchestrationServiceError(string Account, string Message, string Details, string TaskHub, string WorkerName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(202, Account, Message, Details, TaskHub, WorkerName, ExtensionVersion);
        }

        // ----- partition and client lifecycles

        [Event(210, Level = EventLevel.Informational, Version = 1)]
        public void PartitionProgress(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(210, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }

        [Event(211, Level = EventLevel.Warning, Version = 1)]
        public void PartitionWarning(string Account, string TaskHub, int PartitionId, string Context, bool TerminatesPartition, string Message, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(211, Account, TaskHub, PartitionId, Context, TerminatesPartition, Message, Details, ExtensionVersion);
        }

        [Event(212, Level = EventLevel.Error, Version = 1)]
        public void PartitionError(string Account, string TaskHub, int PartitionId, string Context, bool TerminatesPartition, string Message, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(212, Account, TaskHub, PartitionId, Context, TerminatesPartition, Message, Details, ExtensionVersion);
        }

        [Event(213, Level = EventLevel.Informational, Version = 1)]
        public void ClientProgress(string Account, string TaskHub, Guid ClientId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(213, Account, TaskHub, ClientId, Details, ExtensionVersion);
        }

        [Event(214, Level = EventLevel.Warning, Version = 1)]
        public void ClientWarning(string Account, string TaskHub, Guid ClientId, string Context, string Message, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(214, Account, TaskHub, ClientId, Context, Message, Details, ExtensionVersion);
        }

        [Event(215, Level = EventLevel.Error, Version = 1)]
        public void ClientError(string Account, string TaskHub, Guid ClientId, string Context, string Message, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(215, Account, TaskHub, ClientId, Context, Message, Details, ExtensionVersion);
        }

        [Event(216, Level = EventLevel.Verbose, Version = 1)]
        public void ClientTimerProgress(string Account, string TaskHub, Guid ClientId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(216, Account, TaskHub, ClientId, Details, ExtensionVersion);
        }

        [Event(217, Level = EventLevel.Warning, Version = 1)]
        public void ClientRequestTimeout(string Account, string TaskHub, Guid ClientId, string EventId, int PartitionId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(217, Account, TaskHub, ClientId, EventId, PartitionId, ExtensionVersion);
        }

        // ----- specific events relating to DurableTask concepts (TaskMessage, OrchestrationWorkItem, Instance)

        [Event(220, Level = EventLevel.Verbose, Version = 1)]
        public void TaskMessageReceived(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, string EventType, int TaskEventId, string InstanceId, string ExecutionId, string QueuePosition, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(220, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, EventType, TaskEventId, InstanceId, ExecutionId, QueuePosition, ExtensionVersion);
        }

        [Event(221, Level = EventLevel.Verbose, Version = 1)]
        public void TaskMessageSent(string Account, string TaskHub, int PartitionId, string MessageId, string EventType, int TaskEventId, string InstanceId, string ExecutionId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(221, Account, TaskHub, PartitionId, MessageId, EventType, TaskEventId, InstanceId, ExecutionId, ExtensionVersion);
        }

        [Event(222, Level = EventLevel.Warning, Version = 1)]
        public void TaskMessageDiscarded(string Account, string TaskHub, int PartitionId, string MessageId, string Reason, string EventType, int TaskEventId, string InstanceId, string ExecutionId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(222, Account, TaskHub, PartitionId, MessageId, Reason, EventType, TaskEventId, InstanceId, ExecutionId, ExtensionVersion);
        }

        [Event(223, Level = EventLevel.Verbose, Version = 1)]
        public void WorkItemQueued(string Account, string TaskHub, int PartitionId, string WorkItemType, string WorkItemId, string InstanceId, string ExecutionType, string ConsumedMessageIds, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(223, Account, TaskHub, PartitionId, WorkItemType, WorkItemId, InstanceId, ExecutionType, ConsumedMessageIds, ExtensionVersion);
        }

        [Event(224, Level = EventLevel.Verbose, Version = 1)]
        public void WorkItemStarted(string Account, string TaskHub, int PartitionId, string WorkItemType, string WorkItemId, string InstanceId, string ExecutionType, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(224, Account, TaskHub, PartitionId, WorkItemType, WorkItemId, InstanceId, ExecutionType, ExtensionVersion);
        }

        [Event(225, Level = EventLevel.Verbose, Version = 1)]
        public void WorkItemCompleted(string Account, string TaskHub, int PartitionId, string WorkItemType, string WorkItemId, string InstanceId, string Status, string ProducedMessageIds, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(225, Account, TaskHub, PartitionId, WorkItemType, WorkItemId, InstanceId, Status, ProducedMessageIds, ExtensionVersion);
        }

        [Event(226, Level = EventLevel.Warning, Version = 1)]
        public void WorkItemDiscarded(string Account, string TaskHub, int PartitionId, string WorkItemType, string WorkItemId, string InstanceId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(226, Account, TaskHub, PartitionId, WorkItemType, WorkItemId, InstanceId, ExtensionVersion);
        }

        [Event(227, Level = EventLevel.Verbose, Version = 1)]
        public void InstanceUpdated(string Account, string TaskHub, int PartitionId, string InstanceId, string ExecutionId, string WorkItemId, int NewEventCount, int TotalEventCount, string NewEvents, string EventType, int Episode, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(227, Account, TaskHub, PartitionId, InstanceId, ExecutionId, WorkItemId, NewEventCount, TotalEventCount, NewEvents, EventType, Episode, ExtensionVersion);
        }

        [Event(228, Level = EventLevel.Verbose, Version = 1)]
        public void InstanceStatusFetched(string Account, string TaskHub, int PartitionId, string InstanceId, string ExecutionId, string status, string EventId, double LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(228, Account, TaskHub, PartitionId, InstanceId, ExecutionId, status, EventId, LatencyMs, ExtensionVersion);
        }

        [Event(229, Level = EventLevel.Verbose, Version = 1)]
        public void InstanceHistoryFetched(string Account, string TaskHub, int PartitionId, string InstanceId, string ExecutionId, int EventCount, int Episode, string EventId, double LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(229, Account, TaskHub, PartitionId, InstanceId, ExecutionId, EventCount, Episode, EventId, LatencyMs, ExtensionVersion);
        }

        // ----- general event processing and statistics

        [Event(240, Level = EventLevel.Informational, Version = 1)]
        public void PartitionEventProcessed(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, string EventInfo, long NextCommitLogPosition, long NextInputQueuePosition, double QueueLatencyMs, double FetchLatencyMs, double LatencyMs, bool IsReplaying, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(240, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, EventInfo, NextCommitLogPosition, NextInputQueuePosition, QueueLatencyMs, FetchLatencyMs, LatencyMs, IsReplaying, ExtensionVersion);
        }

        [Event(241, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionEventDetail(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(241, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, Details, ExtensionVersion);
        }

        [Event(242, Level = EventLevel.Warning, Version = 1)]
        public void PartitionEventWarning(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(242, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, Details, ExtensionVersion);
        }

        [Event(243, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventReceived(string Account, string TaskHub, Guid ClientId, string MessageId, string EventInfo, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(243, Account, TaskHub, ClientId, MessageId, EventInfo, ExtensionVersion);
        }

        [Event(244, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventSent(string Account, string TaskHub, Guid ClientId, string MessageId, string EventInfo, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(244, Account, TaskHub, ClientId, MessageId, EventInfo, ExtensionVersion);
        }

        [Event(245, Level = EventLevel.Warning, Version = 1)]
        public void PartitionOffloadDecision(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, int ReportedLocalLoad, int Pending, int Backlog, int Remotes, string ReportedRemoteLoad, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(245, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, ReportedLocalLoad, Pending, Backlog, Remotes, ReportedRemoteLoad, ExtensionVersion);
        }

        [Event(246, Level = EventLevel.Informational, Version = 1)]
        public void PartitionLoadPublished(string Account, string TaskHub, int PartitionId, int WorkItems, int Activities, int Timers, int Requests, int Outbox, string NextTimer, long ActivityLatencyMs, long WorkItemLatencyMs, string WorkerId, string LatencyTrend, double MissRate, long InputQueuePosition, long CommitLogPosition, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(246, Account, TaskHub, PartitionId, WorkItems, Activities, Timers, Requests, Outbox, NextTimer, ActivityLatencyMs, WorkItemLatencyMs, WorkerId, LatencyTrend, MissRate, InputQueuePosition, CommitLogPosition, ExtensionVersion);
        }

        // ----- Faster Storage

        [Event(250, Level = EventLevel.Informational, Version = 1)]
        public void FasterStoreCreated(string Account, string TaskHub, int PartitionId, long InputQueuePosition, long LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(250, Account, TaskHub, PartitionId, InputQueuePosition, LatencyMs, ExtensionVersion);
        }

        [Event(251, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointStarted(string Account, string TaskHub, int PartitionId, Guid CheckpointId, string Reason, string StoreStats, long CommitLogPosition, long InputQueuePosition, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(251, Account, TaskHub, PartitionId, CheckpointId, Reason, StoreStats, CommitLogPosition, InputQueuePosition, ExtensionVersion);
        }

        [Event(252, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointPersisted(string Account, string TaskHub, int PartitionId, Guid CheckpointId, string Reason, long CommitLogPosition, long InputQueuePosition, long LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(252, Account, TaskHub, PartitionId, CheckpointId, Reason, CommitLogPosition, InputQueuePosition, LatencyMs, ExtensionVersion);
        }

        [Event(253, Level = EventLevel.Verbose, Version = 1)]
        public void FasterLogPersisted(string Account, string TaskHub, int PartitionId, long CommitLogPosition, long NumberEvents, long SizeInBytes, long LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(253, Account, TaskHub, PartitionId, CommitLogPosition, NumberEvents, SizeInBytes, LatencyMs, ExtensionVersion);
        }

        [Event(254, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointLoaded(string Account, string TaskHub, int PartitionId, long CommitLogPosition, long InputQueuePosition, string StoreStats, long LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(254, Account, TaskHub, PartitionId, CommitLogPosition, InputQueuePosition, StoreStats, LatencyMs, ExtensionVersion);
        }

        [Event(255, Level = EventLevel.Informational, Version = 1)]
        public void FasterLogReplayed(string Account, string TaskHub, int PartitionId, long CommitLogPosition, long InputQueuePosition, long NumberEvents, long SizeInBytes, string StoreStats, long LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(255, Account, TaskHub, PartitionId, CommitLogPosition, InputQueuePosition, NumberEvents, SizeInBytes, StoreStats, LatencyMs, ExtensionVersion);
        }

        [Event(256, Level = EventLevel.Error, Version = 1)]
        public void FasterStorageError(string Account, string TaskHub, int PartitionId, string Context, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(256, Account, TaskHub, PartitionId, Context, Details, ExtensionVersion);
        }

        [Event(257, Level = EventLevel.Error, Version = 1)]
        public void FasterBlobStorageError(string Account, string TaskHub, int PartitionId, string Context, string BlobName, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(257, Account, TaskHub, PartitionId, Context, BlobName, Details, ExtensionVersion);
        }

        [Event(258, Level = EventLevel.Warning, Version = 1)]
        public void FasterBlobStorageWarning(string Account, string TaskHub, int PartitionId, string Context, string BlobName, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(258, Account, TaskHub, PartitionId, Context, BlobName, Details, ExtensionVersion);
        }

        [Event(259, Level = EventLevel.Verbose, Version = 1)]
        public void FasterProgress(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(259, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }

        [Event(260, Level = EventLevel.Informational, Version = 1)]
        public void FasterLeaseAcquired(string Account, string TaskHub, int PartitionId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(260, Account, TaskHub, PartitionId, ExtensionVersion);
        }

        [Event(261, Level = EventLevel.Informational, Version = 1)]
        public void FasterLeaseReleased(string Account, string TaskHub, int PartitionId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(261, Account, TaskHub, PartitionId, ExtensionVersion);
        }

        [Event(262, Level = EventLevel.Warning, Version = 1)]
        public void FasterLeaseLost(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(262, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }

        [Event(263, Level = EventLevel.Verbose, Version = 1)]
        public void FasterLeaseProgress(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(263, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }

        [Event(264, Level = EventLevel.Verbose, Version = 1)]
        public void FasterStorageProgress(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(264, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }

        [Event(265, Level = EventLevel.Warning, Version = 1)]
        public void FasterPerfWarning(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(265, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }


        // ----- EventHubs Transport

        [Event(270, Level = EventLevel.Informational, Version = 1)]
        public void EventHubsInformation(string Account, string TaskHub, string EventHubsNamespace, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(270, Account, TaskHub, EventHubsNamespace, Details, ExtensionVersion);
        }

        [Event(271, Level = EventLevel.Warning, Version = 1)]
        public void EventHubsWarning(string Account, string TaskHub, string EventHubsNamespace, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(271, Account, TaskHub, EventHubsNamespace, Details, ExtensionVersion);
        }

        [Event(272, Level = EventLevel.Error, Version = 1)]
        public void EventHubsError(string Account, string TaskHub, string EventHubsNamespace, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(272, Account, TaskHub, EventHubsNamespace, Details, ExtensionVersion);
        }

        [Event(273, Level = EventLevel.Verbose, Version = 1)]
        public void EventHubsDebug(string Account, string TaskHub, string EventHubsNamespace, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(273, Account, TaskHub, EventHubsNamespace, Details, ExtensionVersion);
        }

        [Event(274, Level = EventLevel.Verbose, Version = 1)]
        public void EventHubsTrace(string Account, string TaskHub, string EventHubsNamespace, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(274, Account, TaskHub, EventHubsNamespace, Details, ExtensionVersion);
        }
    }
}
