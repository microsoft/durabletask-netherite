// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.Extensions.Logging;

    class MemoryStorage : BatchWorker<PartitionEvent>, IPartitionState
    {
        readonly ILogger logger;
        Partition partition;
        long nextSubmitPosition = 0;
        long commitPosition = 0;
        long inputQueuePosition = 0;
        readonly ConcurrentDictionary<TrackedObjectKey, TrackedObject> trackedObjects
            = new ConcurrentDictionary<TrackedObjectKey, TrackedObject>();

        public MemoryStorage(ILogger logger) : base(nameof(MemoryStorage), true, CancellationToken.None)
        {
            this.logger = logger;
            this.GetOrAdd(TrackedObjectKey.Activities);
            this.GetOrAdd(TrackedObjectKey.Dedup);
            this.GetOrAdd(TrackedObjectKey.Outbox);
            this.GetOrAdd(TrackedObjectKey.Reassembly);
            this.GetOrAdd(TrackedObjectKey.Sessions);
            this.GetOrAdd(TrackedObjectKey.Timers);
        }
        public CancellationToken Termination => CancellationToken.None;

        public void SubmitInternalEvent(PartitionEvent entry)
        {
            if (entry is PartitionUpdateEvent updateEvent)
            {
                updateEvent.NextCommitLogPosition = ++this.nextSubmitPosition;
            }

            base.Submit(entry);
        }

        public void SubmitExternalEvents(IList<PartitionEvent> entries)
        {
            foreach (var entry in entries)
            {
                if (entry is PartitionUpdateEvent updateEvent)
                {
                    updateEvent.NextCommitLogPosition = ++this.nextSubmitPosition;
                }
            }

            base.SubmitBatch(entries);
        }

        public Task<long> CreateOrRestoreAsync(Partition partition, IPartitionErrorHandler termination, long initialInputQueuePosition)
        {
            this.partition = partition;

            foreach (var trackedObject in this.trackedObjects.Values)
            {
                trackedObject.Partition = partition;

                if (trackedObject.Key.IsSingleton)
                {
                    trackedObject.OnFirstInitialization();
                }
            }

            this.commitPosition = 1;
            return Task.FromResult(1L);
        }

        public void StartProcessing()
        {
            this.Resume();
        }

        public async Task CleanShutdown(bool takeFinalStateCheckpoint)
        {
            await Task.Delay(10).ConfigureAwait(false);
            
            this.partition.ErrorHandler.TerminateNormally();
        }

        TrackedObject GetOrAdd(TrackedObjectKey key)
        {
            var result = this.trackedObjects.GetOrAdd(key, TrackedObjectKey.Factory);
            result.Partition = this.partition;
            return result;
        }

        IList<OrchestrationState> QueryOrchestrationStates(InstanceQuery query)
        {
            return this.trackedObjects
                .Values
                .Select(trackedObject => trackedObject as InstanceState)
                .Select(instanceState => instanceState?.OrchestrationState)
                .Where(orchestrationState => orchestrationState != null 
                    && (query == null || query.Matches(orchestrationState)))
                .Select(orchestrationState => orchestrationState.ClearFieldsImmutably(query.FetchInput, true))
                .ToList();
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {
                var effects = new EffectTracker(
                    this.partition,
                    this.ApplyToStore,
                    () => (this.commitPosition, this.inputQueuePosition)
                );

                if (batch.Count != 0)
                {
                    foreach (var partitionEvent in batch)
                    {
                        // record the current time, for measuring latency in the event processing pipeline
                        partitionEvent.IssuedTimestamp = this.partition.CurrentTimeMs;

                        try
                        {
                            switch (partitionEvent)
                            {
                                case PartitionUpdateEvent updateEvent:
                                    updateEvent.NextCommitLogPosition = this.commitPosition + 1;
                                    await effects.ProcessUpdate(updateEvent).ConfigureAwait(false);
                                    DurabilityListeners.ConfirmDurable(updateEvent);
                                    if (updateEvent.NextCommitLogPosition > 0)
                                    {
                                        this.partition.Assert(updateEvent.NextCommitLogPosition > this.commitPosition);
                                        this.commitPosition = updateEvent.NextCommitLogPosition;
                                    }
                                    break;

                                case PartitionReadEvent readEvent:
                                    readEvent.OnReadIssued(this.partition);
                                    if (readEvent.Prefetch.HasValue)
                                    {
                                        var prefetchTarget = this.GetOrAdd(readEvent.Prefetch.Value);
                                        effects.ProcessReadResult(readEvent, readEvent.Prefetch.Value, prefetchTarget);
                                    }
                                    var readTarget = this.GetOrAdd(readEvent.ReadTarget);
                                    effects.ProcessReadResult(readEvent, readEvent.ReadTarget, readTarget);
                                    break;

                                case PartitionQueryEvent queryEvent:
                                    var instances = this.QueryOrchestrationStates(queryEvent.InstanceQuery);
                                    var backgroundTask = Task.Run(() => effects.ProcessQueryResultAsync(queryEvent, instances.ToAsyncEnumerable()));
                                    break;

                                default:
                                    throw new InvalidCastException("could not cast to neither PartitionReadEvent nor PartitionUpdateEvent");
                            }

                            if (partitionEvent.NextInputQueuePosition > 0)
                            {
                                this.partition.Assert(partitionEvent.NextInputQueuePosition > this.inputQueuePosition);
                                this.inputQueuePosition = partitionEvent.NextInputQueuePosition;
                            }
                        }
                        catch (Exception e)
                        {
                            this.partition.ErrorHandler.HandleError(nameof(Process), $"Encountered exception while processing event {partitionEvent}", e, false, false);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                this.logger.LogError("Exception in MemoryQueue BatchWorker: {exception}", e);
            }
        }

        public ValueTask ApplyToStore(TrackedObjectKey key, EffectTracker tracker)
        {
            tracker.ProcessEffectOn(this.GetOrAdd(key));
            return default;
        }
    }
}