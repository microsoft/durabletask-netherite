// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#define USE_SECONDARY_INDEX

#pragma warning disable IDE0008 // Use explicit type

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Channels;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using FASTER.core;
    using FASTER.indexes.HashValueIndex;

    class FasterKV : TrackedObjectStore
    {
        readonly FasterKV<Key, Value> fht;

        readonly Partition partition;
        readonly BlobManager blobManager;
        readonly CancellationToken terminationToken;

        ClientSession<Key, Value, EffectTracker, TrackedObject, object, IFunctions<Key, Value, EffectTracker, TrackedObject, object>> mainSession;

#if USE_SECONDARY_INDEX
        readonly HashValueIndex<Key, Value, PredicateKey> secondaryIndex;

        // We currently place all Predicates into a single HashValueIndex with the PredicateKey type
        internal const int SecondaryIndexCount = 1;

        internal IPredicate RuntimeStatusPredicate;
        internal IPredicate CreatedTimePredicate;
        internal IPredicate InstanceIdPrefixPredicate;
#endif
        public FasterKV(Partition partition, BlobManager blobManager)
        {
            this.partition = partition;
            this.blobManager = blobManager;

            partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.fht = new FasterKV<Key, Value>(
                BlobManager.HashTableSize,
                blobManager.StoreLogSettings(partition.Settings.UsePremiumStorage, partition.NumberPartitions()),
                blobManager.StoreCheckpointSettings,
                new SerializerSettings<Key, Value>
                {
                    keySerializer = () => new Key.Serializer(),
                    valueSerializer = () => new Value.Serializer(this.StoreStats),
                });

#if USE_SECONDARY_INDEX
            if (partition.Settings.UseSecondaryIndexQueries)
            {
                int indexOrdinal = 0;
                this.secondaryIndex = new HashValueIndex<Key, Value, PredicateKey>("Netherite", this.fht,
                                            this.blobManager.CreateSecondaryIndexRegistrationSettings<PredicateKey>(partition.NumberPartitions(), indexOrdinal++),
                                            (nameof(this.RuntimeStatusPredicate), v => v.Val is InstanceState state
                                                                                ? new PredicateKey(state.OrchestrationState.OrchestrationStatus)
                                                                                : default),
                                           (nameof(this.CreatedTimePredicate), v => v.Val is InstanceState state
                                                                                ? new PredicateKey(state.OrchestrationState.CreatedTime)
                                                                                : default),
                                           (nameof(this.InstanceIdPrefixPredicate), v => v.Val is InstanceState state
                                                                                ? new PredicateKey(state.InstanceId)
                                                                                : default));

                this.RuntimeStatusPredicate = this.secondaryIndex.GetPredicate(nameof(this.RuntimeStatusPredicate));
                this.CreatedTimePredicate = this.secondaryIndex.GetPredicate(nameof(this.CreatedTimePredicate));
                this.InstanceIdPrefixPredicate = this.secondaryIndex.GetPredicate(nameof(this.InstanceIdPrefixPredicate));
            }
#endif
            this.terminationToken = partition.ErrorHandler.Token;

            var _ = this.terminationToken.Register(
                () => {
                    try
                    {
                        // TODO: Does terminationToken need to call FasterStorage.CancelAndShutdown before releasing fht?
                        this.mainSession?.Dispose();
                        this.secondaryIndex.Dispose();
                        this.fht.Dispose();
                        this.blobManager.HybridLogDevice.Dispose();
                        this.blobManager.ObjectLogDevice.Dispose();
                        this.blobManager.CloseSecondaryIndexDevices();
                    }
                    catch(Exception e)
                    {
                        this.blobManager.TraceHelper.FasterStorageError("Disposing FasterKV", e);
                    }
                }, 
                useSynchronizationContext: false);

            this.blobManager.TraceHelper.FasterProgress("Constructed FasterKV");
        }


        ClientSession<Key, Value, EffectTracker, TrackedObject, object, IFunctions<Key, Value, EffectTracker, TrackedObject, object>> CreateASession()
            => this.fht.NewSession<EffectTracker, TrackedObject, object>(new Functions(this.partition, this.StoreStats));

        public override void InitMainSession() 
            => this.mainSession = this.CreateASession();

        public override async Task<(long commitLogPosition, long inputQueuePosition)> RecoverAsync()
        {
            try
            {
                await this.blobManager.FindCheckpointsAsync();
                this.blobManager.TraceHelper.FasterProgress($"Recovering FasterKV");
                await this.fht.RecoverAsync(numPagesToPreload: 0);
                this.mainSession = this.CreateASession();
                return (this.blobManager.CheckpointInfo.CommitLogPosition, this.blobManager.CheckpointInfo.InputQueuePosition);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override void CompletePending()
        {
            try
            {
                this.mainSession.CompletePending(false, false);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override ValueTask ReadyToCompletePendingAsync()
        {
            return this.mainSession.ReadyToCompletePendingAsync(this.terminationToken);
        }

        public async override ValueTask<Guid?> TakeFullCheckpointAsync(long commitLogPosition, long inputQueuePosition)
        {
            try
            {
                // First do the secondary index(es).
                await this.secondaryIndex.TakeFullCheckpointAsync(CheckpointType.FoldOver);

                this.blobManager.CheckpointInfo.CommitLogPosition = commitLogPosition;
                this.blobManager.CheckpointInfo.InputQueuePosition = inputQueuePosition;
                return this.fht.TakeFullCheckpoint(out var checkpointGuid) ? checkpointGuid : null;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override Guid? StartPrimaryIndexCheckpoint()
        {
            try
            {
                return this.fht.TakeIndexCheckpoint(out var token) ? token : null;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override Guid? StartPrimaryStoreCheckpoint(long commitLogPosition, long inputQueuePosition)
        {
            try
            {
                this.blobManager.CheckpointInfo.CommitLogPosition = commitLogPosition;
                this.blobManager.CheckpointInfo.InputQueuePosition = inputQueuePosition;

                if (this.fht.TakeHybridLogCheckpoint(out var token))
                {
                    // according to Badrish this ensures proper fencing w.r.t. session
                    this.mainSession.Refresh();
                    return token;
                }
                return null;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override async ValueTask CompletePrimaryCheckpointAsync()
        {
            try
            {
                // workaround for hanging in CompleteCheckpointAsync: use custom thread. // TODO: is this still an issue?
                await RunOnDedicatedThreadAsync(() => this.fht.CompleteCheckpointAsync(this.terminationToken).AsTask());
                //await this.fht.CompleteCheckpointAsync(this.terminationToken);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public async static Task RunOnDedicatedThreadAsync(Func<Task> asyncAction)
        {
            Task<Task> tasktask = new Task<Task>(() => asyncAction());
            var thread = new Thread(() => tasktask.RunSynchronously());
            thread.Start();
            await await tasktask;
        }

        public override Task FinalizeCheckpointCompletedAsync(Guid checkpointToken)
        {
            // checkpointToken is not used in this subclass of TrackedObjectStore
            return this.blobManager.FinalizeCheckpointCompletedAsync();
        }

        public override Guid? StartSecondaryIndexIndexCheckpoint()
        {
            try
            {
                return this.secondaryIndex.TakeIndexCheckpoint(out var token) ? token : null;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override Guid? StartSecondaryIndexStoreCheckpoint()
        {
            try
            {
                if (this.secondaryIndex.TakeHybridLogCheckpoint(out var token))
                {
                    // according to Badrish this ensures proper fencing w.r.t. session
                    // this.mainSession.Refresh();  // TODO: revisit this for secondaryIndex
                    return token;
                }
                return null;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override async ValueTask CompleteSecondaryIndexCheckpointAsync()
        {
            try
            {
                // workaround for hanging in CompleteCheckpointAsync: use custom thread. // TODO: is this still an issue?
                await RunOnDedicatedThreadAsync(() => this.secondaryIndex.CompleteCheckpointAsync(this.terminationToken).AsTask());
                //await this.secondaryIndex.CompleteCheckpointAsync(this.terminationToken);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        // perform a query
        public override async Task QueryAsync(PartitionQueryEvent queryEvent, EffectTracker effectTracker)
        {
            try
            {
                var instanceQuery = queryEvent.InstanceQuery;

#if USE_SECONDARY_INDEX
                async IAsyncEnumerable<OrchestrationState> queryIndexAsync(ClientSession<Key, Value, EffectTracker, TrackedObject, object, IFunctions<Key, Value, EffectTracker, TrackedObject, object>> session)
                {
                    var querySettings = new QuerySettings
                    {
                        // This is a match-all-Predicates enumeration so do not continue after any Predicate has hit EOS. Currently we only query the index on a single Predicate, so this is not used.
                        OnStreamEnded = (unusedPredicate, unusedIndex) => false
                    };

                    OrchestrationState getOrchestrationState(ref Value v)
                    {
                        if (v.Val is byte[] serialized)
                        {
                            var result = ((InstanceState)Serializer.DeserializeTrackedObject(serialized))?.OrchestrationState;
                            if (result != null && !instanceQuery.FetchInput)
                            {
                                result.Input = null;
                            }
                            return result;
                        }
                        else
                        {
                            var state = ((InstanceState)((TrackedObject)v))?.OrchestrationState;
                            return state?.ClearFieldsImmutably(instanceQuery.FetchInput, true);
                        }
                    }

                    async IAsyncEnumerable<OrchestrationState> queryPredicate(IPredicate predicate, PredicateKey queryKey)
                    {
                        await foreach (var queryRecord in session.QueryAsync(predicate, queryKey, querySettings).ConfigureAwait(false))
                        {
                            if (instanceQuery.Matches(getOrchestrationState(ref queryRecord.ValueRef)))
                            {
                                yield return getOrchestrationState(ref queryRecord.ValueRef);
                            }
                            queryRecord.Dispose();
                        }
                    }

                    // These are arranged in the order of the expected most-to-least granular property: query the index for that, then post-process the others.
                    if (!string.IsNullOrWhiteSpace(instanceQuery.InstanceIdPrefix))
                    {
                        await foreach (var orcState in queryPredicate(this.InstanceIdPrefixPredicate, new PredicateKey(instanceQuery.InstanceIdPrefix)).ConfigureAwait(false))
                        {
                            yield return orcState;
                        }
                    }
                    else if (instanceQuery.CreatedTimeFrom.HasValue || instanceQuery.CreatedTimeTo.HasValue)
                    {
                        var createdTimeTo = instanceQuery.CreatedTimeTo ?? DateTime.UtcNow;
                        var createdTimeFrom = instanceQuery.CreatedTimeFrom ?? createdTimeTo.AddDays(-7);   // TODO Some default so we don't have to iterate from the first possible date
                        for (var dt = createdTimeFrom; dt <= createdTimeTo; dt += PredicateKey.DateBinInterval)
                        {
                            await foreach (var orcState in queryPredicate(this.CreatedTimePredicate, new PredicateKey(dt)).ConfigureAwait(false))
                            {
                                yield return orcState;
                            }
                        }
                    }
                    else if (instanceQuery.HasRuntimeStatus)
                    {
                        foreach (var status in instanceQuery.RuntimeStatus)
                        { 
                            await foreach (var orcState in queryPredicate(this.RuntimeStatusPredicate, new PredicateKey(status)).ConfigureAwait(false))
                            {
                                yield return orcState;
                            }
                         }
                    }
                }
#else
                IAsyncEnumerable<OrchestrationState> queryIndexAsync(ClientSession<Key, Value, EffectTracker, TrackedObject, object, IFunctions<Key, Value, EffectTracker, TrackedObject, object>> session)
                    => this.ScanOrchestrationStates(effectTracker, queryEvent);
#endif
                // create an individual session for this query so the main session can be used
                // while the query is progressing.
                using (var session = this.CreateASession())
                {
                    var orchestrationStates = (this.partition.Settings.UseSecondaryIndexQueries && instanceQuery.IsSet)
                        ? queryIndexAsync(session)
                        : this.ScanOrchestrationStates(effectTracker, queryEvent);

                    await effectTracker.ProcessQueryResultAsync(queryEvent, orchestrationStates);
                }
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        // kick off a prefetch
        public override async Task RunPrefetchSession(IAsyncEnumerable<TrackedObjectKey> keys)
        {
            int maxConcurrency = 500;
            using SemaphoreSlim prefetchSemaphore = new SemaphoreSlim(maxConcurrency);

            Guid sessionId = Guid.NewGuid();
            this.blobManager.TraceHelper.FasterProgress($"PrefetchSession {sessionId} started (maxConcurrency={maxConcurrency})");

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            long numberIssued = 0;
            long numberMisses = 0;
            long numberHits = 0;
            long lastReport = 0;
            void ReportProgress(int elapsedMillisecondsThreshold)
            {
                if (stopwatch.ElapsedMilliseconds - lastReport >= elapsedMillisecondsThreshold)
                {
                    this.blobManager.TraceHelper.FasterProgress(
                        $"FasterKV PrefetchSession {sessionId} elapsed={stopwatch.Elapsed.TotalSeconds:F2}s issued={numberIssued} pending={maxConcurrency-prefetchSemaphore.CurrentCount} hits={numberHits} misses={numberMisses}");
                    lastReport = stopwatch.ElapsedMilliseconds;
                }
            }

            try
            {
                // these are disposed after the prefetch thread is done
                using var prefetchSession = this.CreateASession();

                // for each key, issue a prefetch
                await foreach (TrackedObjectKey key in keys)
                {
                    // wait for an available prefetch semaphore token
                    while (!await prefetchSemaphore.WaitAsync(50, this.terminationToken))
                    {
                        prefetchSession.CompletePending();
                        ReportProgress(1000);
                    }

                    FasterKV.Key k = key;
                    EffectTracker noInput = null;
                    TrackedObject ignoredOutput = null;
                    var status = prefetchSession.Read(ref k, ref noInput, ref ignoredOutput, userContext: prefetchSemaphore, 0);
                    numberIssued++;

                    switch (status)
                    {
                        case Status.NOTFOUND:
                        case Status.OK:
                            // fast path: we hit in the cache and complete the read
                            numberHits++;
                            prefetchSemaphore.Release();
                            break;

                        case Status.PENDING:
                            // slow path: upon completion
                            numberMisses++;
                            break;

                        case Status.ERROR:
                            this.partition.ErrorHandler.HandleError(nameof(RunPrefetchSession), "FASTER reported ERROR status", null, true, this.partition.ErrorHandler.IsTerminated);
                            break;
                    }

                    this.terminationToken.ThrowIfCancellationRequested();
                    prefetchSession.CompletePending();
                    ReportProgress(1000);
                }

                ReportProgress(0);
                this.blobManager.TraceHelper.FasterProgress($"PrefetchSession {sessionId} is waiting for completion");

                // all prefetches were issued; now we wait for them all to complete
                // by acquiring ALL the semaphore tokens
                for (int i = 0; i < maxConcurrency; i++)
                {
                    while (!await prefetchSemaphore.WaitAsync(50, this.terminationToken))
                    {
                        prefetchSession.CompletePending();
                        ReportProgress(1000);
                    }
                }

                ReportProgress(0);
                this.blobManager.TraceHelper.FasterProgress($"PrefetchSession {sessionId} completed");
            }
            catch (OperationCanceledException) when (this.terminationToken.IsCancellationRequested)
            {
                // partition is terminating
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.partition.ErrorHandler.HandleError(nameof(RunPrefetchSession), "PrefetchSession {sessionId} encountered exception", e, false, this.partition.ErrorHandler.IsTerminated);
            }
        }

        // kick off a read of a tracked object, completing asynchronously if necessary
        public override void ReadAsync(PartitionReadEvent readEvent, EffectTracker effectTracker)
        {
            this.partition.Assert(readEvent != null);
            try
            {
                if (readEvent.Prefetch.HasValue)
                {
                    TryRead(readEvent.Prefetch.Value);
                }

                TryRead(readEvent.ReadTarget);

                void TryRead(Key key)
                {
                    TrackedObject target = null;
                    var status = this.mainSession.Read(ref key, ref effectTracker, ref target, readEvent, 0);
                    switch (status)
                    {
                        case Status.NOTFOUND:
                        case Status.OK:
                            // fast path: we hit in the cache and complete the read
                            this.StoreStats.HitCount++;
                            effectTracker.ProcessReadResult(readEvent, key, target);
                            break;

                        case Status.PENDING:
                            // slow path: read continuation will be called when complete
                            this.StoreStats.MissCount++;
                            break;

                        case Status.ERROR:
                            this.partition.ErrorHandler.HandleError(nameof(ReadAsync), "FASTER reported ERROR status", null, true, this.partition.ErrorHandler.IsTerminated);
                            break;
                    }
                }
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        // read a tracked object on the main session and wait for the response (only one of these is executing at a time)
        public override async ValueTask<TrackedObject> ReadAsync(Key key, EffectTracker effectTracker)
        {
            try
            {
                var result = await this.mainSession.ReadAsync(key, effectTracker, context:null, token: this.terminationToken).ConfigureAwait(false);
                var (status, output) = result.Complete();
                return output;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        // read a tracked object on a query session
        async ValueTask<TrackedObject> ReadAsync(
            ClientSession<Key, Value, EffectTracker, TrackedObject, object, Functions> session,
            Key key, 
            EffectTracker effectTracker)
        {
            try
            {
                var result = await session.ReadAsync(key, effectTracker, context: null, token: this.terminationToken).ConfigureAwait(false);
                var (status, output) = result.Complete();
                return output;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }


        // create a tracked object on the main session (only one of these is executing at a time)
        public async override ValueTask<TrackedObject> CreateAsync(Key key)
        {
            try
            {              
                TrackedObject newObject = TrackedObjectKey.Factory(key);
                newObject.Partition = this.partition;
                Value newValue = newObject;
                var asyncResult = await this.mainSession.UpsertAsync(ref key, ref newValue).ConfigureAwait(false);
                while (asyncResult.Status == Status.PENDING)
                {
                    asyncResult = await asyncResult.CompleteAsync().ConfigureAwait(false);
                }
                return newObject;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public async override ValueTask ProcessEffectOnTrackedObject(Key k, EffectTracker tracker)
        {
            try
            {
                (await this.mainSession.RMWAsync(ref k, ref tracker, token: this.terminationToken)).Complete();
            }
            catch (Exception exception)
               when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override ValueTask RemoveKeys(IEnumerable<TrackedObjectKey> keys)
        {
            foreach (var key in keys)
            {
                this.partition.Assert(!key.IsSingleton);
                this.mainSession.Delete(key);
            }
            return default;
        }

        IAsyncEnumerable<OrchestrationState> ScanOrchestrationStates(
            EffectTracker effectTracker,
            PartitionQueryEvent queryEvent)
        {
            var instanceQuery = queryEvent.InstanceQuery;
            string queryId = queryEvent.EventIdString;
            this.partition.EventDetailTracer?.TraceEventProcessingDetail($"starting query {queryId}");

            // we use a separate thread to iterate, since Faster can iterate synchronously only at the moment
            // and we don't want it to block thread pool worker threads
            var channel = Channel.CreateBounded<OrchestrationState>(500);
            var scanThread = new Thread(RunScan) { Name = $"QueryScan-{queryId}" };
            scanThread.Start();
            return channel.Reader.ReadAllAsync();

            void RunScan()
            {
                using var _ = EventTraceContext.MakeContext(0, queryId);
                using var session = this.CreateASession();

                // get the unique set of keys appearing in the log and emit them
                using var iter1 = session.Iterate();

                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                long scanned = 0;
                long deserialized = 0;
                long matched = 0;
                long lastReport;
                void ReportProgress()
                {
                    this.partition.EventDetailTracer?.TraceEventProcessingDetail(
                        $"query {queryId} scan position={iter1.CurrentAddress} elapsed={stopwatch.Elapsed.TotalSeconds:F2}s scanned={scanned} deserialized={deserialized} matched={matched}");
                    lastReport = stopwatch.ElapsedMilliseconds;
                }

                ReportProgress();

                while (iter1.GetNext(out RecordInfo recordInfo) && !recordInfo.Tombstone)
                {
                    if (stopwatch.ElapsedMilliseconds - lastReport > 5000)
                    {
                        ReportProgress();
                    }

                    TrackedObjectKey key = iter1.GetKey().Val;
                    if (key.ObjectType == TrackedObjectKey.TrackedObjectType.Instance)
                    {
                        scanned++;
                        //this.partition.EventDetailTracer?.TraceEventProcessingDetail($"found instance {key.InstanceId}");

                        if (string.IsNullOrEmpty(instanceQuery?.InstanceIdPrefix)
                            || key.InstanceId.StartsWith(instanceQuery.InstanceIdPrefix))
                        {
                            //this.partition.EventDetailTracer?.TraceEventProcessingDetail($"reading instance {key.InstanceId}");

                            object val = iter1.GetValue().Val;

                            //this.partition.EventDetailTracer?.TraceEventProcessingDetail($"read instance {key.InstanceId}, is {(val == null ? "null" : val.GetType().Name)}");

                            InstanceState instanceState;

                            if (val is byte[] bytes)
                            {
                                instanceState = (InstanceState)Serializer.DeserializeTrackedObject(bytes);
                                deserialized++;
                            }
                            else
                            {
                                instanceState = (InstanceState)val;
                            }

                            // reading the orchestrationState may race with updating the orchestration state
                            // but it is benign because the OrchestrationState object is immutable
                            var orchestrationState = instanceState?.OrchestrationState;

                            if (orchestrationState != null
                                && instanceQuery.Matches(orchestrationState))
                            {
                                matched++;

                                this.partition.EventDetailTracer?.TraceEventProcessingDetail($"match instance {key.InstanceId}");

                                var value = orchestrationState.ClearFieldsImmutably(!instanceQuery.FetchInput, false);

                                var task = channel.Writer.WriteAsync(value);

                                if (!task.IsCompleted)
                                {
                                    task.AsTask().Wait();
                                }
                            }
                        }
                    }
                }

                ReportProgress();

                channel.Writer.Complete();

                this.partition.EventDetailTracer?.TraceEventProcessingDetail($"finished query {queryId}");
            }
        }

        //private async Task<string> DumpCurrentState(EffectTracker effectTracker)    // TODO unused
        //{
        //    try
        //    {
        //        var stringBuilder = new StringBuilder();
        //        await foreach (var trackedObject in EnumerateAllTrackedObjects(effectTracker).OrderBy(obj => obj.Key, new TrackedObjectKey.Comparer()))
        //        {
        //            stringBuilder.Append(trackedObject.ToString());
        //            stringBuilder.AppendLine();
        //        }
        //        return stringBuilder.ToString();
        //    }
        //    catch (Exception exception)
        //        when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
        //    {
        //        throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
        //    }
        //}

        public struct Key : IFasterEqualityComparer<Key>
        {
            public TrackedObjectKey Val;

            public static implicit operator TrackedObjectKey(Key k) => k.Val;
            public static implicit operator Key(TrackedObjectKey k) => new Key() { Val = k };

            public long GetHashCode64(ref Key k)
            {
                unchecked
                {
                    // Compute an FNV hash
                    var hash = 0xcbf29ce484222325ul; // FNV_offset_basis
                    var prime = 0x100000001b3ul; // FNV_prime

                    // hash the kind
                    hash ^= (byte)k.Val.ObjectType;
                    hash *= prime;

                    // hash the instance id, if applicable
                    if (k.Val.InstanceId != null)
                    {
                        for (int i = 0; i < k.Val.InstanceId.Length; i++)
                        {
                            hash ^= k.Val.InstanceId[i];
                            hash *= prime;
                        }
                    }

                    return (long)hash;
                }
            }

            public override string ToString() => this.Val.ToString();

            public bool Equals(ref Key k1, ref Key k2) 
                => k1.Val.ObjectType == k2.Val.ObjectType && k1.Val.InstanceId == k2.Val.InstanceId;

            public class Serializer : BinaryObjectSerializer<Key>
            {
                public override void Deserialize(out Key obj)
                {
                    obj = new Key();
                    obj.Val.Deserialize(this.reader);
                }

                public override void Serialize(ref Key obj) => obj.Val.Serialize(this.writer);
            }
        }

        public struct Value
        {
            public object Val;

            public static implicit operator TrackedObject(Value v) => (TrackedObject)v.Val;
            public static implicit operator Value(TrackedObject v) => new Value() { Val = v };

            public override string ToString() => this.Val.ToString();

            public class Serializer : BinaryObjectSerializer<Value>
            {
                readonly StoreStatistics storeStats;

                public Serializer(StoreStatistics storeStats)
                {
                    this.storeStats = storeStats;
                }

                public override void Deserialize(out Value obj)
                {
                    int count = this.reader.ReadInt32();
                    byte[] bytes = this.reader.ReadBytes(count);
                    var trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                    //if (trackedObject.Key.IsSingleton)
                    //{
                    //    this.storeStats.A++;
                    //    this.storeStats.B += bytes.Length;
                    //}
                    //else if (trackedObject is InstanceState i)
                    //{
                    //    this.storeStats.C++;
                    //    this.storeStats.D += bytes.Length;
                    //    this.storeStats.U.Add(i.InstanceId);
                    //}
                    //else if (trackedObject is HistoryState h)
                    //{
                    //    this.storeStats.E++;
                    //    this.storeStats.F += bytes.Length;
                    //    this.storeStats.UU.Add(h.InstanceId);
                    //}
                    obj = new Value { Val = trackedObject };
                    this.storeStats.Deserialize++;
                }

                public override void Serialize(ref Value obj)
                {
                    if (obj.Val is byte[] serialized)
                    {
                        this.writer.Write(serialized.Length);
                        this.writer.Write(serialized);
                    }
                    else
                    {
                        TrackedObject trackedObject = obj;
                        DurableTask.Netherite.Serializer.SerializeTrackedObject(trackedObject);
                        this.storeStats.Serialize++;
                        this.writer.Write(trackedObject.SerializationCache.Length);
                        this.writer.Write(trackedObject.SerializationCache);
                    }
                }
            }
        }

        public class Functions : IFunctions<Key, Value, EffectTracker, TrackedObject, object>
        {
            readonly Partition partition;
            readonly StoreStatistics stats;

            public Functions(Partition partition, StoreStatistics stats)
            {
                this.partition = partition;
                this.stats = stats;
            }

            public void InitialUpdater(ref Key key, ref EffectTracker tracker, ref Value value)
            {
                var trackedObject = TrackedObjectKey.Factory(key.Val);
                this.stats.Create++;
                trackedObject.Partition = this.partition;
                value.Val = trackedObject;
                tracker.ProcessEffectOn(trackedObject);
                this.stats.Modify++;
            }

            public bool InPlaceUpdater(ref Key key, ref EffectTracker tracker, ref Value value)
            {
                this.partition.Assert(value.Val is TrackedObject);
                TrackedObject trackedObject = value;
                trackedObject.SerializationCache = null; // cache is invalidated
                trackedObject.Partition = this.partition;
                tracker.ProcessEffectOn(trackedObject);
                this.stats.Modify++;
                return true;
            }

            public bool NeedCopyUpdate(ref Key key, ref EffectTracker tracker, ref Value value) => true;

            public void CopyUpdater(ref Key key, ref EffectTracker tracker, ref Value oldValue, ref Value newValue)
            {
                this.stats.Copy++;

                // replace old object with its serialized snapshot
                this.partition.Assert(oldValue.Val is TrackedObject);
                TrackedObject trackedObject = oldValue;
                DurableTask.Netherite.Serializer.SerializeTrackedObject(trackedObject);
                this.stats.Serialize++;
                oldValue.Val = trackedObject.SerializationCache;

                // keep object as the new object, and apply effect
                newValue.Val = trackedObject;
                trackedObject.SerializationCache = null; // cache is invalidated
                trackedObject.Partition = this.partition;
                tracker.ProcessEffectOn(trackedObject);
                this.stats.Modify++;
            }

            public void SingleReader(ref Key key, ref EffectTracker tracker, ref Value value, ref TrackedObject dst)
            {
                if (tracker == null)
                {
                    return; // this is a prefetch, so we don't actually care about the value
                }
                else
                {
                    var trackedObject = value.Val as TrackedObject;
                    this.partition.Assert(trackedObject != null);
                    trackedObject.Partition = this.partition;
                    dst = value;
                    this.stats.Read++;
                }
            }

            public void ConcurrentReader(ref Key key, ref EffectTracker tracker, ref Value value, ref TrackedObject dst)
            {
                if (tracker == null)
                {
                    return; // this is a prefetch, so we don't actually care about the value
                }
                else
                {
                    var trackedObject = value.Val as TrackedObject;
                    this.partition.Assert(trackedObject != null);
                    trackedObject.Partition = this.partition;
                    dst = value;
                    this.stats.Read++;
                }
            }

            public void SingleWriter(ref Key key, ref Value src, ref Value dst)
            {
                dst.Val = src.Val;
            }

            public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst)
            {
                dst.Val = src.Val;
                return true;
            }

            public void ReadCompletionCallback(ref Key key, ref EffectTracker tracker, ref TrackedObject output, object context, Status status)
            {
                if (context == null)
                {
                    // no need to take any action here
                }
                else if (tracker == null)
                {
                    // this is a prefetch
                    ((SemaphoreSlim)context).Release();
                }
                else
                {
                    // the result is passed on to the read event
                    var partitionReadEvent = (PartitionReadEvent)context;
                    switch (status)
                    {
                        case Status.NOTFOUND:
                            tracker.ProcessReadResult(partitionReadEvent, key, null);
                            break;

                        case Status.OK:
                            tracker.ProcessReadResult(partitionReadEvent, key, output);
                            break;

                        case Status.PENDING:
                            this.partition.ErrorHandler.HandleError(nameof(ReadCompletionCallback), "invalid FASTER result code", null, true, false);
                            break;

                        case Status.ERROR:
                            this.partition.ErrorHandler.HandleError(nameof(ReadCompletionCallback), "FASTER reported ERROR status", null, true, this.partition.ErrorHandler.IsTerminated);
                            break;
                    }
                }
            }

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
            public void RMWCompletionCallback(ref Key key, ref EffectTracker input, object ctx, Status status) { }
            public void UpsertCompletionCallback(ref Key key, ref Value value, object ctx) { }
            public void DeleteCompletionCallback(ref Key key, object ctx) { }

            public bool SupportsLocking => false;   // TODO - implement locking?

            public void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, ref long lockContext)
            {
            }

            public bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, long lockContext)
            {
                return true;
            }
        }
    }
}
