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
    using DurableTask.Netherite.Abstractions;
    using DurableTask.Netherite.Faster;
    using DurableTask.Netherite.Scaling;
    using Microsoft.Extensions.Logging;

    class MemoryStorage : BatchWorker<PartitionEvent>, IPartitionState
    {
        readonly ILogger logger;
        readonly ConcurrentDictionary<TrackedObjectKey, TrackedObject> trackedObjects;

        Partition partition;
        EffectTracker effects;
        long commitPosition = 0;
        (long,int) inputQueuePosition =(0,0);

        public MemoryStorage(ILogger logger) : base(nameof(MemoryStorageLayer), true, int.MaxValue, CancellationToken.None, null)
        {
            this.logger = logger;
            this.trackedObjects = new ConcurrentDictionary<TrackedObjectKey, TrackedObject>();
            foreach (var k in TrackedObjectKey.GetSingletons())
            {
                this.GetOrAdd(k);
            }
        }
 
        public CancellationToken Termination => CancellationToken.None;

        public void SubmitEvent(PartitionEvent entry)
        {
            base.Submit(entry);
        }

        public void SubmitParallelEvent(PartitionEvent entry)
        {
            base.Submit(entry);
        }

        public void SubmitEvents(IList<PartitionEvent> entries)
        {
            base.SubmitBatch(entries);
        }

        public async Task<(long,int)> CreateOrRestoreAsync(Partition partition, IPartitionErrorHandler termination, string fingerprint)
        {
            await Task.Yield();
            this.partition = partition;
            this.effects = new MemoryStorageEffectTracker(partition, this);

            foreach (var trackedObject in this.trackedObjects.Values)
            {
                trackedObject.Partition = partition;

                if (trackedObject.Key.IsSingleton)
                {
                    trackedObject.OnFirstInitialization(partition);
                }
            }

            this.commitPosition = 1;
            this.inputQueuePosition = (0,0);
            return this.inputQueuePosition;
        }

        public void StartProcessing()
        {
            this.Resume();
        }

        public async Task CleanShutdown(bool takeFinalStateCheckpoint)
        {
            await Task.Yield();    
            this.partition.ErrorHandler.TerminateNormally();
        }

        TrackedObject GetOrAdd(TrackedObjectKey key)
        {
            var result = this.trackedObjects.GetOrAdd(key, TrackedObjectKey.Factory);
            result.Partition = this.partition;
            return result;
        }

        IEnumerable<(string, OrchestrationState)> QueryOrchestrationStates(InstanceQuery query, int pageSize, string continuationToken)
        {
            var instances = this.trackedObjects
                .Values
                .Select(trackedObject => trackedObject as InstanceState)
                .Select(instanceState => instanceState?.OrchestrationState)
                .Where(orchestrationState =>
                    orchestrationState != null
                    && orchestrationState.OrchestrationInstance.InstanceId.CompareTo(continuationToken) > 0
                    && (query == null || query.Matches(orchestrationState)))
                .OrderBy(orchestrationState => orchestrationState.OrchestrationInstance.InstanceId)
                .Select(orchestrationState => orchestrationState.ClearFieldsImmutably(!query.FetchInput, false))
                .Append(null)
                .Take(pageSize == 0 ? int.MaxValue : pageSize);

            string last = "";

            foreach (OrchestrationState instance in instances)
            {
                if (instance != null)
                {
                    last = instance.OrchestrationInstance.InstanceId;
                    yield return (last, instance);
                }
                else
                {
                    yield return (null, null);
                    yield break;
                }
            }

            yield return (last, null);
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
                                        this.partition.Assert(updateEvent.NextCommitLogPosition > this.commitPosition, "updateEvent.NextCommitLogPosition > this.commitPosition in MemoryStorage.Process");
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
                                    var instances = this.QueryOrchestrationStates(queryEvent.InstanceQuery, queryEvent.PageSize, queryEvent.ContinuationToken ?? "");

                                    var backgroundTask = Task.Run(() => this.effects.ProcessQueryResultAsync(queryEvent, instances.ToAsyncEnumerable(), DateTime.UtcNow));
                                    break;

                                default:
                                    throw new InvalidCastException("could not cast to neither PartitionReadEvent nor PartitionUpdateEvent");
                            }

                            if (partitionEvent.NextInputQueuePosition > 0)
                            {
                                this.partition.Assert(partitionEvent.NextInputQueuePositionTuple.CompareTo(this.inputQueuePosition) > 0, "partitionEvent.NextInputQueuePosition > this.inputQueuePosition in MemoryStorage");
                                this.inputQueuePosition = partitionEvent.NextInputQueuePositionTuple;
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

            public override (long, (long,int)) GetPositions()
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