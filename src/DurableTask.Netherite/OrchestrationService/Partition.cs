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

        public double CurrentTimeMs => this.stopwatch.Elapsed.TotalMilliseconds;

        public NetheriteOrchestrationServiceSettings Settings { get; private set; }
        public string StorageAccountName { get; private set; }

        public IPartitionState State { get; private set; }
        public TransportAbstraction.ISender BatchSender { get; private set; }
        public WorkItemQueue<ActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        public WorkItemQueue<OrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }
        public LoadPublisher LoadPublisher { get; private set; }

        public BatchTimer<PartitionEvent> PendingTimers { get; private set; }

        public EventTraceHelper EventTraceHelper { get; }

        public bool RecoveryIsComplete { get; private set; }

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
            LoadPublisher loadPublisher)
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
            this.TraceHelper = new PartitionTraceHelper(host.Logger, settings.LogLevelLimit, this.StorageAccountName, this.Settings.HubName, this.PartitionId);
            this.EventTraceHelper = new EventTraceHelper(host.LoggerFactory, settings.EventLogLevelLimit, this);
            this.stopwatch.Start();
        }

        public async Task<long> CreateOrRestoreAsync(IPartitionErrorHandler errorHandler, long firstInputQueuePosition)
        {
            EventTraceContext.Clear();

            this.ErrorHandler = errorHandler;
            this.TraceHelper.TraceProgress("Starting partition");

            await MaxConcurrentStarts.WaitAsync();

            // create or restore partition state from last snapshot
            try
            {
                // create the state
                this.State = ((IStorageProvider)this.host).CreatePartitionState();

                // initialize timer for this partition
                this.PendingTimers = new BatchTimer<PartitionEvent>(this.ErrorHandler.Token, this.TimersFired);

                // goes to storage to create or restore the partition state
                this.TraceHelper.TraceProgress("Loading partition state");
                var inputQueuePosition = await this.State.CreateOrRestoreAsync(this, this.ErrorHandler, firstInputQueuePosition).ConfigureAwait(false);

                this.RecoveryIsComplete = true;

                this.PendingTimers.Start($"Timer{this.PartitionId:D2}");

                this.TraceHelper.TraceProgress($"Started partition, nextInputQueuePosition={inputQueuePosition}");
                return inputQueuePosition;
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

        public async Task StopAsync(bool isForced)
        {
            this.TraceHelper.TraceProgress("Stopping partition");

            try
            {
                if (!this.ErrorHandler.IsTerminated)
                {
                    bool takeCheckpoint = this.Settings.TakeStateCheckpointWhenStoppingPartition && !isForced;
                    // for a clean shutdown we try to save some of the latest progress to storage and then release the lease
                    await this.State.CleanShutdown(takeCheckpoint).ConfigureAwait(false);
                }
            }
            catch(OperationCanceledException) when (this.ErrorHandler.IsTerminated)
            {
                // o.k. during termination
            }
            catch (Exception e)
            {
                this.ErrorHandler.HandleError(nameof(StopAsync), "Could not shut down partition state cleanly", e, true, false);
            }

            // at this point, the partition has been terminated (either cleanly or by exception)
            this.Assert(this.ErrorHandler.IsTerminated);

            // tell the load publisher to send all buffered info
            this.LoadPublisher?.Flush();

            this.TraceHelper.TraceProgress("Stopped partition");
        }

        void TimersFired(List<PartitionEvent> timersFired)
        {
            try
            {
                foreach (var t in timersFired)
                {
                    switch(t)
                    {
                        case PartitionUpdateEvent updateEvent:
                            this.SubmitInternalEvent(updateEvent);
                            break;
                        case PartitionReadEvent readEvent:
                            this.SubmitInternalEvent(readEvent);
                            break;
                        case PartitionQueryEvent queryEvent:
                            this.SubmitInternalEvent(queryEvent);
                            break;
                        default:
                            throw new InvalidCastException("Could not cast to neither PartitionUpdateEvent nor PartitionReadEvent");
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

            // trace DTFx TaskMessages that are sent to other participants
            if (this.RecoveryIsComplete)
            {
                foreach (var taskMessage in updateEvent.TracedTaskMessages)
                {
                    this.EventTraceHelper.TraceTaskMessageSent(taskMessage, updateEvent.EventIdString);
                }
            }

            this.BatchSender.Submit(updateEvent);
        }

        public void SubmitInternalEvent(PartitionUpdateEvent updateEvent)
        {
            // for better analytics experience, trace DTFx TaskMessages that are "sent" 
            // by this partition to itself the same way as if sent to other partitions
            if (this.RecoveryIsComplete)
            {
                foreach (var taskMessage in updateEvent.TracedTaskMessages)
                {
                    this.EventTraceHelper.TraceTaskMessageSent(taskMessage, updateEvent.EventIdString);
                }
            }

            updateEvent.ReceivedTimestamp = this.CurrentTimeMs;

            this.State.SubmitInternalEvent(updateEvent);
        }

        public void SubmitInternalEvent(PartitionReadEvent readEvent)
        {
            readEvent.ReceivedTimestamp = this.CurrentTimeMs;
            this.State.SubmitInternalEvent(readEvent);
        }

        public void SubmitInternalEvent(PartitionQueryEvent queryEvent)
        {
            queryEvent.ReceivedTimestamp = this.CurrentTimeMs;
            this.State.SubmitInternalEvent(queryEvent);
        }

        public void SubmitExternalEvents(IList<PartitionEvent> partitionEvents)
        {
            foreach (PartitionEvent partitionEvent in partitionEvents)
            {
                partitionEvent.ReceivedTimestamp = this.CurrentTimeMs;
            }

            this.State.SubmitExternalEvents(partitionEvents);
        }

        public void EnqueueActivityWorkItem(ActivityWorkItem item)
        {
            this.EventDetailTracer?.TraceEventProcessingDetail($"Enqueueing ActivityWorkItem {item.WorkItemId}");
            this.ActivityWorkItemQueue.Add(item);
        }
 
        public void EnqueueOrchestrationWorkItem(OrchestrationWorkItem item)
        { 
            this.EventDetailTracer?.TraceEventProcessingDetail($"Enqueueing OrchestrationWorkItem {item.MessageBatch.WorkItemId}");
            this.OrchestrationWorkItemQueue.Add(item);
        }
    }
}
