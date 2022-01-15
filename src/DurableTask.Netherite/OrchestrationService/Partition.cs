// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracing;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Extensions.Logging;

    partial class Partition : TransportAbstraction.IPartition
    {
        readonly NetheriteOrchestrationService host;
        readonly Stopwatch stopwatch = new Stopwatch();

        public uint PartitionId { get; private set; }
        public string TracePrefix { get; private set; }
        public Func<string, uint> PartitionFunction { get; private set; }
        public Func<uint> NumberPartitions { get; private set; }
        public IPartitionErrorHandler ErrorHandler { get; private set; }
        public PartitionTraceHelper TraceHelper { get; private set; }
        public WorkItemTraceHelper WorkItemTraceHelper { get; private set; }

        public Faster.MemoryTracker CacheTracker { get; private set; }

        public double CurrentTimeMs => this.stopwatch.Elapsed.TotalMilliseconds;

        public double LastTransition;

        public NetheriteOrchestrationServiceSettings Settings { get; private set; }
        public string StorageAccountName { get; private set; }

        public IPartitionState State { get; private set; }
        public TransportAbstraction.ISender BatchSender { get; private set; }
        public WorkItemQueue<ActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        public WorkItemQueue<OrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }
        public LoadPublisher LoadPublisher { get; private set; }

        public BatchTimer<PartitionEvent> PendingTimers { get; private set; }

        public EventTraceHelper EventTraceHelper { get; }

        // A little helper property that allows us to conventiently check the condition for low-level event tracing
        public EventTraceHelper EventDetailTracer => this.EventTraceHelper.IsTracingAtMostDetailedLevel ? this.EventTraceHelper : null;

        static readonly SemaphoreSlim MaxConcurrentStarts = new SemaphoreSlim(5);

        public Partition(
            NetheriteOrchestrationService host,
            uint partitionId,
            Func<string, uint> partitionFunction,
            Func<uint> numberPartitions,
            TransportAbstraction.ISender batchSender,
            NetheriteOrchestrationServiceSettings settings,
            string storageAccountName,
            WorkItemQueue<ActivityWorkItem> activityWorkItemQueue,
            WorkItemQueue<OrchestrationWorkItem> orchestrationWorkItemQueue,
            LoadPublisher loadPublisher,

            WorkItemTraceHelper workItemTraceHelper)
        {
            this.host = host;
            this.PartitionId = partitionId;
            this.PartitionFunction = partitionFunction;
            this.NumberPartitions = numberPartitions;
            this.BatchSender = batchSender;
            this.Settings = settings;
            this.StorageAccountName = storageAccountName;
            this.ActivityWorkItemQueue = activityWorkItemQueue;
            this.OrchestrationWorkItemQueue = orchestrationWorkItemQueue;
            this.LoadPublisher = loadPublisher;
            this.TraceHelper = new PartitionTraceHelper(host.TraceHelper.Logger, settings.LogLevelLimit, this.StorageAccountName, this.Settings.HubName, this.PartitionId);
            this.EventTraceHelper = new EventTraceHelper(host.LoggerFactory, settings.EventLogLevelLimit, this);
            this.WorkItemTraceHelper = workItemTraceHelper;
            this.stopwatch.Start();
            this.LastTransition = this.CurrentTimeMs;
        }

        public async Task<long> CreateOrRestoreAsync(IPartitionErrorHandler errorHandler, long firstInputQueuePosition)
        {
            EventTraceContext.Clear();

            this.ErrorHandler = errorHandler;

            this.TraceHelper.TracePartitionProgress("Starting", ref this.LastTransition, this.CurrentTimeMs, "");

            errorHandler.Token.Register(() => this.TraceHelper.TracePartitionProgress("Terminated", ref this.LastTransition, this.CurrentTimeMs, ""));

            await MaxConcurrentStarts.WaitAsync();

            // create or restore partition state from last snapshot
            try
            {
                // create the state
                this.State = ((IStorageProvider)this.host).CreatePartitionState();

                // initialize timer for this partition
                this.PendingTimers = new BatchTimer<PartitionEvent>(this.ErrorHandler.Token, this.TimersFired);

                // goes to storage to create or restore the partition state
                var inputQueuePosition = await this.State.CreateOrRestoreAsync(this, this.ErrorHandler, firstInputQueuePosition).ConfigureAwait(false);

                // start processing the timers
                this.PendingTimers.Start($"Timer{this.PartitionId:D2}");

                // start processing the worker queues
                this.State.StartProcessing();

                this.TraceHelper.TracePartitionProgress("Started", ref this.LastTransition, this.CurrentTimeMs, $"nextInputQueuePosition={inputQueuePosition}");
                return inputQueuePosition;
            }
            catch (OperationCanceledException) when (errorHandler.IsTerminated)
            {
                // this happens when startup is canceled
                throw;
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.ErrorHandler.HandleError(nameof(CreateOrRestoreAsync), "Could not start partition", e, true, false);
                throw;
            }
            finally
            {
                MaxConcurrentStarts.Release();
            }
        }

        [Conditional("DEBUG")]
        public void Assert(bool condition)
        {
            if (!condition)
            {
                if (System.Diagnostics.Debugger.IsAttached)
                {
                    System.Diagnostics.Debugger.Break();
                }

                var stacktrace = System.Environment.StackTrace;

                this.ErrorHandler.HandleError(stacktrace, "Assertion failed", null, false, false);
            }
        }

        public async Task StopAsync(bool quickly)
        {
            if (!this.ErrorHandler.IsTerminated)
            {
                this.TraceHelper.TracePartitionProgress("Stopping", ref this.LastTransition, this.CurrentTimeMs, $"quickly={quickly}");

                bool takeCheckpoint = this.Settings.TakeStateCheckpointWhenStoppingPartition && !quickly;

                // for a clean shutdown we try to save some of the latest progress to storage and then release the lease
                bool clean = true;
                try
                {
                    await this.State.CleanShutdown(takeCheckpoint).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (this.ErrorHandler.IsTerminated)
                {
                    // o.k. during termination
                }
                catch (Exception e)
                {
                    this.ErrorHandler.HandleError(nameof(StopAsync), "Could not shut down partition state cleanly", e, true, false);
                    clean = false;
                }

                // at this point, the partition has been terminated (either cleanly or by exception)
                this.Assert(this.ErrorHandler.IsTerminated);

                // tell the load publisher to send all buffered info
                if (this.LoadPublisher != null)
                {
                    await this.LoadPublisher.FlushAsync();
                }

                this.TraceHelper.TracePartitionProgress("Stopped", ref this.LastTransition, this.CurrentTimeMs, $"takeCheckpoint={takeCheckpoint} clean={clean}");
            }
        }

        void TimersFired(List<PartitionEvent> timersFired)
        {
            try
            {
                foreach (var t in timersFired)
                {
                    switch(t)
                    {
                        case TimerFired timerFired:
                            this.SubmitEvent(timerFired);
                            this.WorkItemTraceHelper.TraceTaskMessageSent(this.PartitionId, timerFired.TaskMessage, timerFired.OriginWorkItemId, null, null);
                            break;
                        case PartitionUpdateEvent updateEvent:
                            this.SubmitEvent(updateEvent);
                            break;
                        case PartitionReadEvent readEvent:
                            this.SubmitEvent(readEvent);
                            break;
                        case PartitionQueryEvent queryEvent:
                            this.SubmitParallelEvent(queryEvent);
                            break;
                        default:
                            throw new InvalidCastException("Could not cast to any appropriate type of event");
                    }
                }
            }
            catch (OperationCanceledException) when (this.ErrorHandler.IsTerminated)
            {
                // o.k. during termination
            }
            catch (Exception e)
            {
                this.ErrorHandler.HandleError("TimersFired", "Encountered exception while firing partition timers", e, true, false);
            }
        }

        public void Send(ClientEvent clientEvent)
        {
            this.EventDetailTracer?.TraceEventProcessingDetail($"Sending client event {clientEvent} id={clientEvent.EventId}");
            this.BatchSender.Submit(clientEvent);
        }

        public void Send(PartitionUpdateEvent updateEvent)
        {
            this.EventDetailTracer?.TraceEventProcessingDetail($"Sending partition update event {updateEvent} id={updateEvent.EventId}");
            this.BatchSender.Submit(updateEvent);
        }

        public void Send(LoadMonitorEvent loadMonitorEvent)
        {
            this.EventDetailTracer?.TraceEventProcessingDetail($"Sending load monitor event {loadMonitorEvent} id={loadMonitorEvent.EventId}");
            this.BatchSender.Submit(loadMonitorEvent);
        }

        public void SubmitEvent(PartitionUpdateEvent updateEvent)
        {
            updateEvent.ReceivedTimestamp = this.CurrentTimeMs;
            this.State.SubmitEvent(updateEvent);
            updateEvent.OnSubmit(this);
        }

        public void SubmitEvent(PartitionReadEvent readEvent)
        {
            readEvent.ReceivedTimestamp = this.CurrentTimeMs;
            this.State.SubmitEvent(readEvent);
        }

        public void SubmitParallelEvent(PartitionEvent partitionEvent)
        {
            this.Assert(!(partitionEvent is PartitionUpdateEvent));
            partitionEvent.ReceivedTimestamp = this.CurrentTimeMs;
            this.State.SubmitParallelEvent(partitionEvent);
        }

        public void SubmitEvents(IList<PartitionEvent> partitionEvents)
        {
            foreach (PartitionEvent partitionEvent in partitionEvents)
            {
                partitionEvent.ReceivedTimestamp = this.CurrentTimeMs;
            }

            this.State.SubmitEvents(partitionEvents);

            foreach (PartitionEvent partitionEvent in partitionEvents)
            {
                partitionEvent.OnSubmit(this);
            }
        }

        public void EnqueueActivityWorkItem(ActivityWorkItem item)
        {
            this.Assert(!string.IsNullOrEmpty(item.OriginWorkItem));

            this.WorkItemTraceHelper.TraceWorkItemQueued(
                this.PartitionId,
                WorkItemTraceHelper.WorkItemType.Activity,
                item.WorkItemId,
                item.TaskMessage.OrchestrationInstance.InstanceId,
                item.ExecutionType,
                0,
                WorkItemTraceHelper.FormatMessageId(item.TaskMessage, item.OriginWorkItem));

            this.ActivityWorkItemQueue.Add(item);
        }
 
        public void EnqueueOrchestrationWorkItem(OrchestrationWorkItem item)
        {
            this.WorkItemTraceHelper.TraceWorkItemQueued(
                this.PartitionId,
                WorkItemTraceHelper.WorkItemType.Orchestration,
                item.MessageBatch.WorkItemId,
                item.InstanceId,
                item.Type.ToString(),
                item.HistorySize,
                WorkItemTraceHelper.FormatMessageIdList(item.MessageBatch.TracedMessages));

            item.PreStatus = item.OrchestrationRuntimeState.Status;
            this.OrchestrationWorkItemQueue.Add(item);
        }
    }
}
