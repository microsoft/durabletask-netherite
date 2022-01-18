// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
        EffectTracker effects;
        long nextSubmitPosition = 0;
        long commitPosition = 0;
        long inputQueuePosition = 0;
        readonly ConcurrentDictionary<TrackedObjectKey, TrackedObject> trackedObjects
            = new ConcurrentDictionary<TrackedObjectKey, TrackedObject>();

        public MemoryStorage(ILogger logger) : base(nameof(MemoryStorage), true, int.MaxValue, CancellationToken.None, null)
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

        public void SubmitEvent(PartitionEvent entry)
        {
            if (entry is PartitionUpdateEvent updateEvent)
            {
                updateEvent.NextCommitLogPosition = ++this.nextSubmitPosition;
            }

            base.Submit(entry);
        }

        public void SubmitParallelEvent(PartitionEvent entry)
        {
            base.Submit(entry);
        }

        public void SubmitEvents(IList<PartitionEvent> entries)
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
            this.effects = new MemoryStorageEffectTracker(partition, this);

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
                .Select(orchestrationState => orchestrationState.ClearFieldsImmutably(!query.FetchInput, false))
                .ToList();
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {             
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
                                    await this.effects.ProcessUpdate(updateEvent).ConfigureAwait(false);
                                    DurabilityListeners.ConfirmDurable(updateEvent);
                                    if (updateEvent.NextCommitLogPosition > 0)
                                    {
                                        this.partition.Assert(updateEvent.NextCommitLogPosition > this.commitPosition);
                                        this.commitPosition = updateEvent.NextCommitLogPosition;
                                    }
                                    break;

                                case PartitionReadEvent readEvent:
                                    if (readEvent.Prefetch.HasValue)
                                    {
                                        var prefetchTarget = this.GetOrAdd(readEvent.Prefetch.Value);
                                        this.effects.ProcessReadResult(readEvent, readEvent.Prefetch.Value, prefetchTarget);
                                    }
                                    var readTarget = this.GetOrAdd(readEvent.ReadTarget);
                                    this.effects.ProcessReadResult(readEvent, readEvent.ReadTarget, readTarget);
                                    break;

                                case PartitionQueryEvent queryEvent:
                                    var instances = this.QueryOrchestrationStates(queryEvent.InstanceQuery);
                                    var backgroundTask = Task.Run(() => this.effects.ProcessQueryResultAsync(queryEvent, instances.ToAsyncEnumerable()));
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

        public Task Prefetch(IEnumerable<TrackedObjectKey> keys)
        {
            return Task.CompletedTask;
        }

        class MemoryStorageEffectTracker : PartitionEffectTracker
        {
            readonly MemoryStorage memoryStorage;

            public MemoryStorageEffectTracker(Partition partition, MemoryStorage memoryStorage)
                : base(partition)
            { 
                this.memoryStorage = memoryStorage;
            }

            public override ValueTask ApplyToStore(TrackedObjectKey key, EffectTracker tracker)
            {
                tracker.ProcessEffectOn(this.memoryStorage.GetOrAdd(key));
                return default;
            }

            public override (long, long) GetPositions()
            {
                return (this.memoryStorage.commitPosition, this.memoryStorage.inputQueuePosition);
            }

            public override ValueTask RemoveFromStore(IEnumerable<TrackedObjectKey> keys)
            {
                foreach (var key in keys)
                {
                    this.memoryStorage.trackedObjects.TryRemove(key, out _);
                }
                return default;
            }
        }
    }
}