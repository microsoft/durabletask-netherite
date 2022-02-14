// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Channels;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Tracing;
    using FASTER.core;
    using Microsoft.Azure.Storage.Blob.Protocol;
    using Newtonsoft.Json;

    class FasterKV : TrackedObjectStore
    {
        readonly FasterKV<Key, Value> fht;

        readonly Partition partition;
        readonly BlobManager blobManager;
        readonly CancellationToken terminationToken;
        readonly CacheDebugger cacheDebugger;
        readonly MemoryTracker.CacheTracker cacheTracker;
        readonly LogSettings storelogsettings;

        TrackedObject[] singletons;
        Task persistSingletonsTask;

        ClientSession<Key, Value, EffectTracker, Output, object, IFunctions<Key, Value, EffectTracker, Output, object>> mainSession;

        public FasterTraceHelper TraceHelper => this.blobManager.TraceHelper;

        public int PageSizeBits => this.storelogsettings.PageSizeBits;

        public FasterKV(Partition partition, BlobManager blobManager, MemoryTracker memoryTracker)
        {
            this.partition = partition;
            this.blobManager = blobManager;
            this.cacheDebugger = partition.Settings.TestHooks?.CacheDebugger;

            partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.storelogsettings = blobManager.GetDefaultStoreLogSettings(
                partition.Settings.UseSeparatePageBlobStorage, 
                memoryTracker.MaxCacheSize, 
                partition.Settings.FasterTuningParameters);

            this.fht = new FasterKV<Key, Value>(
                BlobManager.HashTableSize,
                this.storelogsettings,
                blobManager.StoreCheckpointSettings,
                new SerializerSettings<Key, Value>
                {
                    keySerializer = () => new Key.Serializer(),
                    valueSerializer = () => new Value.Serializer(this.StoreStats, partition.TraceHelper, this.cacheDebugger),
                });

            this.cacheTracker = memoryTracker.NewCacheTracker(this, (int) partition.PartitionId, this.cacheDebugger);

            this.fht.Log.SubscribeEvictions(new EvictionObserver(this));
            this.fht.Log.Subscribe(new ReadonlyObserver(this));

            partition.Assert(this.fht.ReadCache == null, "Unexpected read cache");

            this.terminationToken = partition.ErrorHandler.Token;

            var _ = this.terminationToken.Register(() => Task.Run(this.Dispose));

            this.blobManager.TraceHelper.FasterProgress("Constructed FasterKV");
        }

        public void Dispose()
        {
            try
            {
                this.cacheTracker?.Dispose();
                this.mainSession?.Dispose();
                this.fht.Dispose();
                this.blobManager.HybridLogDevice.Dispose();
                this.blobManager.ObjectLogDevice.Dispose();
                this.blobManager.ClosePSFDevices();
                this.blobManager.FaultInjector?.Disposed(this.blobManager);
            }
            catch (Exception e)
            {
                this.blobManager.TraceHelper.FasterStorageError("Disposing FasterKV", e);
            }
        }

        ClientSession<Key, Value, EffectTracker, Output, object, IFunctions<Key, Value, EffectTracker, Output, object>> CreateASession(string id, bool isScan)
        {
            var functions = new Functions(this.partition, this, this.cacheTracker, isScan);
            return this.fht.NewSession(functions, id);
        }

        string RandomSuffix() => Guid.NewGuid().ToString().Substring(0, 5);

        public LogAccessor<Key, Value> Log => this.fht?.Log;

        public override void InitMainSession()
        {
            this.singletons = new TrackedObject[TrackedObjectKey.NumberSingletonTypes];
            this.mainSession = this.CreateASession($"main-{this.RandomSuffix()}", false);
            this.cacheTracker.MeasureCacheSize();
            this.CheckInvariants();
        }

        public override async Task<(long commitLogPosition, long inputQueuePosition)> RecoverAsync()
        {
            try
            {
                await this.blobManager.FindCheckpointsAsync();

                // recover singletons
                this.blobManager.TraceHelper.FasterProgress($"Recovering Singletons");
                using (var stream = await this.blobManager.RecoverSingletonsAsync())
                {
                    this.singletons = Serializer.DeserializeSingletons(stream);
                }
                foreach (var singleton in this.singletons)
                {
                    singleton.Partition = this.partition;
                }

                // recover Faster
                this.blobManager.TraceHelper.FasterProgress($"Recovering FasterKV");
                await this.fht.RecoverAsync();
                this.mainSession = this.CreateASession($"main-{this.RandomSuffix()}", false);
                this.cacheTracker.MeasureCacheSize();
                this.CheckInvariants();

                return (this.blobManager.CheckpointInfo.CommitLogPosition, this.blobManager.CheckpointInfo.InputQueuePosition);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override bool CompletePending()
        {
            try
            {
                return this.mainSession.CompletePending(false, false);
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

        public override bool TakeFullCheckpoint(long commitLogPosition, long inputQueuePosition, out Guid checkpointGuid)
        {
            try
            {
                this.blobManager.CheckpointInfo.CommitLogPosition = commitLogPosition;
                this.blobManager.CheckpointInfo.InputQueuePosition = inputQueuePosition;
                if (this.fht.TryInitiateFullCheckpoint(out checkpointGuid, CheckpointType.FoldOver))
                {
                    byte[] serializedSingletons = Serializer.SerializeSingletons(this.singletons);
                    this.persistSingletonsTask = this.blobManager.PersistSingletonsAsync(serializedSingletons, checkpointGuid);
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override async ValueTask CompleteCheckpointAsync()
        {
            try
            {
                // workaround for hanging in CompleteCheckpointAsync: use custom thread.
                await RunOnDedicatedThreadAsync(() => this.fht.CompleteCheckpointAsync(this.terminationToken).AsTask());
                //await this.fht.CompleteCheckpointAsync(this.terminationToken);

                await this.persistSingletonsTask;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override async Task RemoveObsoleteCheckpoints()
        {
            await this.blobManager.RemoveObsoleteCheckpoints();
        }

        public async static Task RunOnDedicatedThreadAsync(Func<Task> asyncAction)
        {
            Task<Task> tasktask = new Task<Task>(() => asyncAction());
            var thread = new Thread(() => tasktask.RunSynchronously());
            thread.Start();
            await await tasktask;
        }

        public override Task FinalizeCheckpointCompletedAsync(Guid guid)
        {
            return this.blobManager.FinalizeCheckpointCompletedAsync();
        }

        public override Guid? StartIndexCheckpoint()
        {
            try
            {
                if (this.fht.TryInitiateIndexCheckpoint(out var token))
                {
                    this.persistSingletonsTask = Task.CompletedTask;
                    return token;
                }
                else
                {
                    return null;
                }
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override Guid? StartStoreCheckpoint(long commitLogPosition, long inputQueuePosition, long? shiftBeginAddress)
        {
            try
            {
                this.blobManager.CheckpointInfo.CommitLogPosition = commitLogPosition;
                this.blobManager.CheckpointInfo.InputQueuePosition = inputQueuePosition;

                if (shiftBeginAddress > this.fht.Log.BeginAddress)
                {
                    this.fht.Log.ShiftBeginAddress(shiftBeginAddress.Value);
                }

                if (this.fht.TryInitiateHybridLogCheckpoint(out var token, CheckpointType.FoldOver))
                {
                    // according to Badrish this ensures proper fencing w.r.t. session
                    this.mainSession.Refresh();

                    byte[] serializedSingletons = Serializer.SerializeSingletons(this.singletons);
                    this.persistSingletonsTask = this.blobManager.PersistSingletonsAsync(serializedSingletons, token);

                    return token;
                }
                else
                {
                    return null;
                }

                throw new InvalidOperationException("Faster refused store checkpoint");
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override long? GetCompactionTarget()
        {
            // TODO empiric validation of the heuristics

            var stats = (StatsState) this.singletons[(int)TrackedObjectKey.Stats.ObjectType];
            long minimalLogSize = this.fht.Log.FixedRecordSize * stats.InstanceCount * 2;
            long actualLogSize = this.fht.Log.TailAddress - this.fht.Log.BeginAddress;

            if (actualLogSize < 4 * minimalLogSize)
            {
                return null; // we are not compacting until there is significant bloat
            }

            long compactionAreaSize = (long)(0.2 * (this.fht.Log.SafeReadOnlyAddress - this.fht.Log.BeginAddress));
            long mutableSectionSize = (this.fht.Log.TailAddress - this.fht.Log.SafeReadOnlyAddress);

            if (mutableSectionSize > compactionAreaSize)
            {
                return null; // the potential size reduction does not outweigh the cost of a foldover
            }

            return this.fht.Log.BeginAddress + compactionAreaSize;
        }

        readonly static SemaphoreSlim maxCompactionThreads = new SemaphoreSlim((Environment.ProcessorCount + 1) / 2);

        public override async Task<long> RunCompactionAsync(long target)
        {
            string id = this.RandomSuffix(); // for tracing purposes
            this.blobManager.TraceHelper.FasterProgress($"Compaction {id} pending");

            await maxCompactionThreads.WaitAsync();
            try
            {
                var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
                var thread = new Thread(Run);
                thread.Name = $"Compaction.{id}";
                thread.Start();
                return await tcs.Task;

                void Run()
                {
                    this.blobManager.TraceHelper.FasterProgress($"Compaction {id} started");

                    try
                    {
                        using var session = this.CreateASession($"compaction-{id}", false);
                        long compactedUntil = session.Compact(target, CompactionType.Lookup);
                        tcs.SetResult(compactedUntil);
                    }
                    catch (Exception exception)
                        when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
                    {
                        tcs.SetException(new OperationCanceledException("Partition was terminated.", exception, this.terminationToken));
                    }
                    catch (Exception e)
                    {
                        tcs.SetException(e);
                    }
                }
            }
            finally
            {
                maxCompactionThreads.Release();
                this.blobManager.TraceHelper.FasterProgress($"Compaction {id} done");
            }
        }

        // perform a query
        public override async Task QueryAsync(PartitionQueryEvent queryEvent, EffectTracker effectTracker)
        {
            try
            {
                var orchestrationStates = this.ScanOrchestrationStates(effectTracker, queryEvent);
                await effectTracker.ProcessQueryResultAsync(queryEvent, orchestrationStates);
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
                using var prefetchSession = this.CreateASession($"prefetch-{this.RandomSuffix()}", false);

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
                    Output ignoredOutput = default;
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

        // kick off a read of a tracked object on the main session, completing asynchronously if necessary
        public override void Read(PartitionReadEvent readEvent, EffectTracker effectTracker)
        {
            this.partition.Assert(readEvent != null, "null readEvent in ReadAsync");
            try
            {
                if (readEvent.Prefetch.HasValue)
                {
                    TryRead(readEvent.Prefetch.Value);
                }

                TryRead(readEvent.ReadTarget);

                void TryRead(Key key)
                {
                    this.partition.Assert(!key.Val.IsSingleton, "singletons are not read asynchronously");
                    Output output = default;
                    this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.StartingRead, null, readEvent.EventIdString, 0);
                    var status = this.mainSession.Read(ref key, ref effectTracker, ref output, readEvent, 0);
                    switch (status)
                    {
                        case Status.NOTFOUND:
                        case Status.OK:
                            // fast path: we hit in the cache and complete the read
                            this.StoreStats.HitCount++;
                            this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.CompletedRead, null, readEvent.EventIdString, 0);
                            var target = output.Read(this, readEvent.EventIdString);
                            this.cacheDebugger?.CheckVersionConsistency(key.Val, target, null);
                            effectTracker.ProcessReadResult(readEvent, key, target);
                            break;

                        case Status.PENDING:
                            // slow path: read continuation will be called when complete
                            this.StoreStats.MissCount++;
                            this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PendingRead, null, readEvent.EventIdString, 0);
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
        public override ValueTask<TrackedObject> ReadAsync(Key key, EffectTracker effectTracker)
        {
            this.partition.Assert(key.Val.IsSingleton, "only singletons expected in ReadAsync");
            return new ValueTask<TrackedObject>(this.singletons[(int)key.Val.ObjectType]);
        }

        // create a tracked object on the main session (only one of these is executing at a time)
        public override ValueTask<TrackedObject> CreateAsync(Key key)
        {
            this.partition.Assert(key.Val.IsSingleton, "only singletons expected in CreateAsync");
            try
            {
                TrackedObject newObject = TrackedObjectKey.Factory(key);
                newObject.Partition = this.partition;
                this.singletons[(int)key.Val.ObjectType] = newObject;
                return new ValueTask<TrackedObject>(newObject);
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
                if (k.Val.IsSingleton)
                {
                    tracker.ProcessEffectOn(this.singletons[(int)k.Val.ObjectType]);
                }
                else
                {
                    this.cacheDebugger?.Record(k, CacheDebugger.CacheEvent.StartingRMW, null, tracker.CurrentEventId, 0);

                    await this.PerformFasterRMWAsync(k, tracker);

                    this.cacheDebugger?.Record(k, CacheDebugger.CacheEvent.CompletedRMW, null, tracker.CurrentEventId, 0);
                }
            }
            catch (Exception exception)
               when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        async ValueTask PerformFasterRMWAsync(Key k, EffectTracker tracker)
        {
            int numTries = 10;

            while (true)
            {
                try
                {
                    var rmwAsyncResult = await this.mainSession.RMWAsync(ref k, ref tracker, token: this.terminationToken);

                    bool IsComplete()
                    {
                        switch (rmwAsyncResult.Status)
                        {
                            case Status.NOTFOUND:
                            case Status.OK:
                                return true;

                            case Status.PENDING:
                                return false;

                            case Status.ERROR:
                            default:
                                string msg = $"Could not execute RMW in Faster, received status={rmwAsyncResult.Status}";
                                this.cacheDebugger?.Fail(msg, k);
                                throw new FasterException(msg);
                        }
                    }

                    if (IsComplete())
                    {
                        return;
                    }

                    while (true)
                    {
                        this.cacheDebugger?.Record(k, CacheDebugger.CacheEvent.PendingRMW, null, tracker.CurrentEventId, 0);

                        rmwAsyncResult = await rmwAsyncResult.CompleteAsync();

                        if (IsComplete())
                        {
                            return;
                        }

                        if (--numTries == 0)
                        {
                            this.cacheDebugger?.Fail($"Failed to execute RMW in Faster: status={rmwAsyncResult.Status.ToString()}", k);
                            throw new FasterException("Could not complete RMW even after all retries");
                        }
                    }
                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    if (--numTries == 0)
                    {
                        this.cacheDebugger?.Fail($"Failed to execute RMW in Faster, encountered exception: {exception}", k);
                        throw;
                    }
                }
            }
        }

        public override ValueTask RemoveKeys(IEnumerable<TrackedObjectKey> keys)
        {
            foreach (var key in keys)
            {
                this.partition.Assert(!key.IsSingleton, "singletons cannot be deleted");
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
                using var session = this.CreateASession($"scan-{queryId}-{this.RandomSuffix()}", true);

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

                while (iter1.GetNext(out RecordInfo recordInfo, out Key key, out Value val) && !recordInfo.Tombstone)
                {
                    if (stopwatch.ElapsedMilliseconds - lastReport > 5000)
                    {
                        ReportProgress();
                    }

                    if (key.Val.ObjectType == TrackedObjectKey.TrackedObjectType.Instance)
                    {
                        scanned++;
                        //this.partition.EventDetailTracer?.TraceEventProcessingDetail($"found instance {key.InstanceId}");

                        if (string.IsNullOrEmpty(instanceQuery?.InstanceIdPrefix)
                            || key.Val.InstanceId.StartsWith(instanceQuery.InstanceIdPrefix))
                        {
                            //this.partition.EventDetailTracer?.TraceEventProcessingDetail($"reading instance {key.InstanceId}");

                            //this.partition.EventDetailTracer?.TraceEventProcessingDetail($"read instance {key.InstanceId}, is {(val == null ? "null" : val.GetType().Name)}");

                            InstanceState instanceState;

                            if (val.Val is byte[] bytes)
                            {
                                instanceState = (InstanceState)Serializer.DeserializeTrackedObject(bytes);
                                deserialized++;
                            }
                            else
                            {
                                instanceState = (InstanceState)val.Val;
                            }

                            // reading the orchestrationState may race with updating the orchestration state
                            // but it is benign because the OrchestrationState object is immutable
                            var orchestrationState = instanceState?.OrchestrationState;

                            if (orchestrationState != null
                                && instanceQuery.Matches(orchestrationState))
                            {
                                matched++;

                                this.partition.EventDetailTracer?.TraceEventProcessingDetail($"match instance {key.Val.InstanceId}");

                                var task = channel.Writer.WriteAsync(orchestrationState);

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

        public override void EmitCurrentState(Action<TrackedObjectKey, TrackedObject> emitItem)
        {
            try
            {
                var stringBuilder = new StringBuilder();

                // iterate singletons
                foreach(var key in TrackedObjectKey.GetSingletons())
                {
                    var singleton = this.singletons[(int)key.ObjectType];
                    emitItem(key, singleton);
                }

                using var session = this.CreateASession($"emitCurrentState-{this.RandomSuffix()}", true);

                // iterate histories
                using (var iter1 = session.Iterate())
                {
                    while (iter1.GetNext(out RecordInfo recordInfo, out var key, out var value) && !recordInfo.Tombstone)
                    {
                        TrackedObject trackedObject;
                        if (value.Val == null)
                        {
                            trackedObject = null;
                        }
                        else if (value.Val is TrackedObject t)
                        {
                            trackedObject = t;
                        }
                        else if (value.Val is byte[] bytes)
                        {
                            trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                        }
                        else
                        {
                            throw new InvalidCastException("cannot cast value to TrackedObject");
                        }

                        this.cacheDebugger?.CheckVersionConsistency(key, trackedObject, value.Version);
                        emitItem(key, trackedObject);
                    }
                }
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public long MemoryUsedWithoutObjects => this.fht.IndexSize * 64 + this.fht.Log.MemorySizeBytes + this.fht.OverflowBucketCount * 64;

        public override (double totalSizeMB, int fillPercentage) CacheSizeInfo {
            get 
            {
                double totalSize = (double)(this.cacheTracker.TrackedObjectSize + this.MemoryUsedWithoutObjects);
                double targetSize = (double) this.cacheTracker.TargetSize;
                int fillPercentage = (int) Math.Round(100 * (totalSize / targetSize));
                double totalSizeMB = Math.Round(100 * totalSize / (1024 * 1024)) / 100;
                return (totalSizeMB, fillPercentage);
            }
        }

        public override void AdjustCacheSize()
        {
            this.cacheTracker.Notify();
        }

        public override void CheckInvariants()
        {
            this.ValidateMemoryTracker();
        }

        public void ValidateMemoryTracker()
        {
            if (this.cacheDebugger == null)
            {
                return; // we only do this when the cache debugger is attached
            }

            var inMemoryIterator = this.fht.Log.Scan(this.fht.Log.HeadAddress, this.fht.Log.TailAddress);

            long totalSize = 0;
            Dictionary<TrackedObjectKey, List<(long delta, long address, string desc)>> perKey = new Dictionary<TrackedObjectKey, List<(long delta, long address, string desc)>>();
            void Add(TrackedObjectKey key, long delta, long address, string desc)
            {
                perKey.TryGetValue(key, out var current);
                if (current == null)
                {
                    current = perKey[key] = new List<(long delta, long address, string desc)>();
                }       
                current.Add((delta, address, desc));
                totalSize += delta;
            }

            
            while (inMemoryIterator.GetNext(out RecordInfo recordInfo, out Key key, out Value value))
            {
                long delta = key.Val.EstimatedSize;
                if (!recordInfo.Tombstone)
                {
                    delta += value.EstimatedSize;
                }
                Add(key, delta, inMemoryIterator.CurrentAddress, $"{(recordInfo.Invalid ? "I" : "")}{(recordInfo.Tombstone ? "T" : "")}{delta}@{inMemoryIterator.CurrentAddress.ToString("x")}");
            }

            long trackedSize = this.cacheTracker.TrackedObjectSize;

            bool sizeMatches = true;
            foreach (var kvp in perKey)
            {
                sizeMatches = sizeMatches && this.cacheDebugger.CheckSize(kvp.Key, kvp.Value, this.Log.HeadAddress);
            }

            if (trackedSize != totalSize && sizeMatches)
            {
                this.cacheDebugger.Fail("total size of tracked objects does not match");
            }
        }

        internal (int numPages, long size) ComputeMemorySize(bool resetCacheDebugger)
        {
            var cacheDebugger = resetCacheDebugger ? this.cacheDebugger : null;
            cacheDebugger?.Reset((string instanceId) => this.partition.PartitionFunction(instanceId) == this.partition.PartitionId);
            var inMemoryIterator = this.fht.Log.Scan(this.fht.Log.HeadAddress, this.fht.Log.TailAddress);
            long totalSize = 0;
            long firstPage = this.fht.Log.HeadAddress >> this.storelogsettings.PageSizeBits;
            while (inMemoryIterator.GetNext(out RecordInfo recordInfo, out Key key, out Value value))
            {
                long delta = key.Val.EstimatedSize;
                if (!recordInfo.Tombstone)
                {
                    delta += value.EstimatedSize;
                }
                totalSize += delta;
                cacheDebugger?.UpdateSize(key, delta);
            }
            long lastPage = this.fht.Log.TailAddress >> this.storelogsettings.PageSizeBits;
            return ((int) (lastPage-firstPage) + 1, totalSize);
        }

        public void SetEmptyPageCount(int emptyPageCount)
        {
            this.fht.Log.SetEmptyPageCount(emptyPageCount, true);
        }

        class EvictionObserver : IObserver<IFasterScanIterator<Key, Value>>
        {
            readonly FasterKV store;
            public EvictionObserver(FasterKV store)
            {
                this.store = store;
            }

            public void OnCompleted() { }
            public void OnError(Exception error) { }

            public void OnNext(IFasterScanIterator<Key, Value> iterator)
            {
                long totalSize = 0;
                while (iterator.GetNext(out RecordInfo recordInfo, out Key key, out Value value))
                {
                    long size;
                    if (!recordInfo.Tombstone)
                    {
                        this.store.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.Evict, value.Version, null, iterator.CurrentAddress);
                        size = key.Val.EstimatedSize + value.EstimatedSize;
                    }
                    else
                    {
                        this.store.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.EvictTombstone, null, null, iterator.CurrentAddress);
                        size = key.Val.EstimatedSize;
                    }

                    this.store.cacheDebugger?.UpdateTrackedObjectSize(-size, key, iterator.CurrentAddress);
                    totalSize += size;
                }
                this.store.TraceHelper.FasterStorageProgress($"Evicted until address={iterator.EndAddress}");
                this.store.cacheTracker.OnEviction(totalSize, iterator.EndAddress);
            }
        }

        class ReadonlyObserver : IObserver<IFasterScanIterator<Key, Value>>
        {
            readonly FasterKV store;
            public ReadonlyObserver(FasterKV store)
            {
                this.store = store;
            }

            public void OnCompleted() { }
            public void OnError(Exception error) { }

            public void OnNext(IFasterScanIterator<Key, Value> iterator)
            {
                while (iterator.GetNext(out RecordInfo recordInfo, out Key key, out Value value))
                {
                    if (!recordInfo.Tombstone)
                    {
                        this.store.cacheDebugger?.Record(key, CacheDebugger.CacheEvent.Readonly, value.Version, null, iterator.CurrentAddress);
                    }
                    else
                    {
                        this.store.cacheDebugger?.Record(key, CacheDebugger.CacheEvent.ReadonlyTombstone, null, null, iterator.CurrentAddress);
                    }
                }
            }
        }

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

            public int Version; // we use this validate consistency of read/write updates in FASTER, it is not otherwise needed

            public static implicit operator Value(TrackedObject v) => new Value() { Val = v };

            public override string ToString() => this.Val.ToString();

            public long EstimatedSize => 8 + (
                this.Val is byte[] bytes ? 40 + bytes.Length :
                this.Val is TrackedObject o ? o.EstimatedSize :
                0);

            public class Serializer : BinaryObjectSerializer<Value>
            {
                readonly StoreStatistics storeStats;
                readonly PartitionTraceHelper traceHelper;
                readonly CacheDebugger cacheDebugger;

                public Serializer(StoreStatistics storeStats, PartitionTraceHelper traceHelper, CacheDebugger cacheDebugger)
                {
                    this.storeStats = storeStats;
                    this.traceHelper = traceHelper;
                    this.cacheDebugger = cacheDebugger;
                }

                public override void Deserialize(out Value obj)
                {
                    int version = this.reader.ReadInt32();
                    int count = this.reader.ReadInt32();
                    byte[] bytes = this.reader.ReadBytes(count); // lazy deserialization - keep as byte array until used
                    obj = new Value { Val = bytes, Version = version};
                    if (this.cacheDebugger != null)
                    {
                        var trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                        this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeBytes, version, null, 0);
                    }
                }

                public override void Serialize(ref Value obj)
                {
                    this.writer.Write(obj.Version);
                    if (obj.Val is byte[] serialized)
                    {
                        // We did already serialize this object on the last CopyUpdate. So we can just use the byte array.
                        this.writer.Write(serialized.Length);
                        this.writer.Write(serialized);
                        if (this.cacheDebugger != null)
                        {
                            var trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(serialized);
                            this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.SerializeBytes, obj.Version, null, 0);
                        }
                    }
                    else
                    {
                        TrackedObject trackedObject = (TrackedObject) obj.Val;
                        DurableTask.Netherite.Serializer.SerializeTrackedObject(trackedObject);
                        this.storeStats.Serialize++;
                        this.writer.Write(trackedObject.SerializationCache.Length);
                        this.writer.Write(trackedObject.SerializationCache);
                        this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.SerializeObject, obj.Version, null, 0);
                    }
                }
            }
        }

        public struct Output
        {
            public object Val;

            public TrackedObject Read(FasterKV store, string eventId)
            {
                if (this.Val == null)
                {
                    return null;
                }
                else if (this.Val is TrackedObject trackedObject)
                {
                    return trackedObject;
                }
                else
                {
                    byte[] bytes = this.Val as byte[];
                    store.partition.Assert(bytes != null, "unexpected type in Output.Read");
                    trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                    store.StoreStats.Deserialize++;
                    store.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, null, eventId, 0);
                    trackedObject.Partition = store.partition;
                    return trackedObject;
                }
            }
        }

        public class Functions : IFunctions<Key, Value, EffectTracker, Output, object>
        {
            readonly Partition partition;
            readonly FasterKV store;
            readonly StoreStatistics stats;
            readonly CacheDebugger cacheDebugger;
            readonly MemoryTracker.CacheTracker cacheTracker;
            readonly bool isScan;

            public Functions(Partition partition, FasterKV store, MemoryTracker.CacheTracker cacheTracker, bool isScan)
            {
                this.partition = partition;
                this.store = store;
                this.stats = store.StoreStats;
                this.cacheDebugger = partition.Settings.TestHooks?.CacheDebugger;
                this.cacheTracker = isScan ? null : cacheTracker;
                this.isScan = isScan;
            }
            
            // for use with ITraceListener on a modified FASTER branch with extra instrumentation
            //public void TraceKey(Key key, string message)
            //{
            //    this.cacheDebugger?.Record(key, CacheDebugger.CacheEvent.Faster, null, message, 0);
            //}
            //public void TraceRequest(Key key, long id, string message)
            //{
            //    this.cacheDebugger?.Record(key, CacheDebugger.CacheEvent.Faster, null, $"{id:D10}-{message}", 0);
            //    this.store.TraceHelper.FasterStorageProgress($"FASTER: {id:D10}-{message} key={key}");
            //}
            //public void Trace(long id, string message)
            //{
            //    this.store.TraceHelper.FasterStorageProgress($"FASTER: {id:D10}-{message}");
            //}

            bool IFunctions<Key, Value, EffectTracker, Output, object>.NeedInitialUpdate(ref Key key, ref EffectTracker input, ref Output output)
                => true;

            void IFunctions<Key, Value, EffectTracker, Output, object>.InitialUpdater(ref Key key, ref EffectTracker tracker, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.InitialUpdate, 0, tracker.CurrentEventId, address);
                this.cacheDebugger?.ValidateObjectVersion(value, key.Val);
                this.cacheDebugger?.CheckVersionConsistency(key.Val, null, value.Version);
                var trackedObject = TrackedObjectKey.Factory(key.Val);
                this.stats.Create++;
                trackedObject.Partition = this.partition;
                value.Val = trackedObject;
                tracker.ProcessEffectOn(trackedObject);
                value.Version++;
                this.cacheDebugger?.UpdateReferenceValue(ref key.Val, trackedObject, value.Version);
                this.stats.Modify++;
                this.partition.Assert(value.Val != null, "null value.Val in InitialUpdater");
                this.partition.Assert(!this.isScan, "InitialUpdater should not be called from scan");
                this.cacheDebugger?.ValidateObjectVersion(value, key.Val);
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.PostInitialUpdater(ref Key key, ref EffectTracker tracker, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PostInitialUpdate, value.Version, tracker.CurrentEventId, address);
                // we have inserted a new entry at the tail
                this.cacheTracker.UpdateTrackedObjectSize(key.Val.EstimatedSize + value.EstimatedSize, key, address);
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.InPlaceUpdater(ref Key key, ref EffectTracker tracker, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.InPlaceUpdate, value.Version, tracker.CurrentEventId, address);
                this.cacheDebugger?.ValidateObjectVersion(value, key.Val);
                long sizeBeforeUpdate = value.EstimatedSize;
                if (!(value.Val is TrackedObject trackedObject))
                {
                    var bytes = (byte[])value.Val;
                    this.partition.Assert(bytes != null, "null bytes in InPlaceUpdater");
                    trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                    this.stats.Deserialize++;
                    value.Val = trackedObject;
                    this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, value.Version, tracker.CurrentEventId, 0);
                }
                trackedObject.SerializationCache = null; // cache is invalidated because of update
                trackedObject.Partition = this.partition;
                this.cacheDebugger?.CheckVersionConsistency(key.Val, trackedObject, value.Version);
                tracker.ProcessEffectOn(trackedObject);
                value.Version++;
                this.cacheDebugger?.UpdateReferenceValue(ref key.Val, trackedObject, value.Version);
                this.stats.Modify++;
                this.partition.Assert(value.Val != null, "null value.Val in InPlaceUpdater");
                this.cacheTracker.UpdateTrackedObjectSize(value.EstimatedSize - sizeBeforeUpdate, key, address);
                this.cacheDebugger?.ValidateObjectVersion(value, key.Val);
                this.partition.Assert(!this.isScan, "InPlaceUpdater should not be called from scan");
                return true;
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.NeedCopyUpdate(ref Key key, ref EffectTracker tracker, ref Value value, ref Output output)
                => true;

            void IFunctions<Key, Value, EffectTracker, Output, object>.CopyUpdater(ref Key key, ref EffectTracker tracker, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.CopyUpdate, oldValue.Version, tracker.CurrentEventId, address);
                this.cacheDebugger?.ValidateObjectVersion(oldValue, key.Val);

                if (oldValue.Val is TrackedObject trackedObject)
                {
                    // replace old object with its serialized snapshot
                    long oldValueSizeBefore = oldValue.EstimatedSize;
                    DurableTask.Netherite.Serializer.SerializeTrackedObject(trackedObject);
                    this.stats.Serialize++;
                    this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.SerializeObject, oldValue.Version, null, 0);
                    oldValue.Val = trackedObject.SerializationCache;
                    this.cacheTracker.UpdateTrackedObjectSize(oldValue.EstimatedSize - oldValueSizeBefore, key, null); // null indicates we don't know the address
                    this.stats.Copy++;
                }
                else
                {
                    // create new object by deserializing old object
                    var bytes = (byte[])oldValue.Val;
                    this.partition.Assert(bytes != null, "null bytes in CopyUpdater");
                    trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                    this.stats.Deserialize++;
                    trackedObject.Partition = this.partition;
                    this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, oldValue.Version, tracker.CurrentEventId, 0);
                }

                newValue.Val = trackedObject;
                trackedObject.SerializationCache = null; // cache is invalidated by the update which is happening below
                this.cacheDebugger?.CheckVersionConsistency(key.Val, trackedObject, oldValue.Version);
                tracker.ProcessEffectOn(trackedObject);
                newValue.Version = oldValue.Version + 1;
                this.cacheDebugger?.UpdateReferenceValue(ref key.Val, trackedObject, newValue.Version);
                this.stats.Modify++;
                this.partition.Assert(newValue.Val != null, "null newValue.Val in CopyUpdater");
                this.cacheDebugger?.ValidateObjectVersion(oldValue, key.Val);
                this.cacheDebugger?.ValidateObjectVersion(newValue, key.Val);
                this.partition.Assert(!this.isScan, "CopyUpdater should not be called from scan");
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.PostCopyUpdater(ref Key key, ref EffectTracker tracker, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address)
            {
                // Note: Post operation is called only when cacheDebugger is attached.
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PostCopyUpdate, newValue.Version, tracker.CurrentEventId, address);
                this.cacheTracker.UpdateTrackedObjectSize(key.Val.EstimatedSize + newValue.EstimatedSize, key, address);
                return true;
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.SingleReader(ref Key key, ref EffectTracker tracker, ref Value src, ref Output dst, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.SingleReader, src.Version, default, address);
                this.cacheDebugger?.ValidateObjectVersion(src, key.Val);
           
                if (src.Val == null)
                {
                    dst.Val = null;
                }
                else if (src.Val is byte[] bytes)
                {
                    var trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                    this.stats.Deserialize++;
                    this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, src.Version, null, 0);
                    trackedObject.Partition = this.partition;
                    dst.Val = trackedObject;
                }
                else if (src.Val is TrackedObject trackedObject)
                {
                    if (!this.isScan)
                    {
                        // replace src with a serialized snapshot of the object, so it does not get mutated
                        long oldValueSizeBefore = src.EstimatedSize;
                        DurableTask.Netherite.Serializer.SerializeTrackedObject(trackedObject);
                        this.stats.Serialize++;
                        this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.SerializeObject, src.Version, null, 0);
                        src.Val = trackedObject.SerializationCache;
                        this.cacheTracker.UpdateTrackedObjectSize(src.EstimatedSize - oldValueSizeBefore, key, address);
                        this.stats.Copy++;
                    }
                    dst.Val = trackedObject;
                }

                this.stats.Read++;
                return true;
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.ConcurrentReader(ref Key key, ref EffectTracker tracker, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.ConcurrentReader, value.Version, default, address);
                this.cacheDebugger?.ValidateObjectVersion(value, key.Val);

                TrackedObject trackedObject = null;
                if (value.Val != null)
                {
                    if (value.Val is byte[] bytes)
                    {
                        this.cacheDebugger?.Fail("Unexpected byte[] state in mutable section");

                        // we should never get here but for robustness we still continue as best as possible
                        trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                        this.stats.Deserialize++;
                        trackedObject.Partition = this.partition;
                        this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, value.Version, default, 0);
                    }
                    else
                    {
                        trackedObject = (TrackedObject)value.Val;
                        this.partition.Assert(trackedObject != null, "null trackedObject in Reader");
                    }
                }

                dst.Val = trackedObject;
                this.stats.Read++;
                return true;
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.SingleWriter(ref Key key, ref EffectTracker input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address, WriteReason reason)
            {
                switch (reason)
                {
                    case WriteReason.Upsert:
                        this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.SingleWriterUpsert, src.Version, default, address);
                        if (!this.isScan)
                        {
                            this.cacheDebugger?.Fail("Do not expect SingleWriter-Upsert outside of scans", key);
                        }
                        break;

                    case WriteReason.CopyToReadCache:
                        this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.SingleWriterCopyToReadCache, src.Version, default, address);
                        this.cacheDebugger?.Fail("Do not expect SingleWriter-CopyToReadCache", key);
                        break;

                    case WriteReason.CopyToTail:
                        this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.SingleWriterCopyToTail, src.Version, default, address);
                        break;

                    case WriteReason.Compaction:
                        this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.SingleWriterCompaction, src.Version, default, address);
                        break;
                }
                dst.Val = output.Val ?? src.Val;
                dst.Version = src.Version;
                this.cacheDebugger?.ValidateObjectVersion(dst, key.Val);
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.PostSingleWriter(ref Key key, ref EffectTracker input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address, WriteReason reason)
            {
                switch (reason)
                {
                    case WriteReason.Upsert:
                        this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PostSingleWriterUpsert, src.Version, default, address);
                        if (!this.isScan)
                        {
                            this.cacheDebugger?.Fail("Do not expect PostSingleWriter-Upsert outside of scans", key);
                        }
                        break;

                    case WriteReason.CopyToReadCache:
                        this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PostSingleWriterCopyToReadCache, src.Version, default, address);
                        this.cacheDebugger?.Fail("Do not expect PostSingleWriter-CopyToReadCache", key);
                        break;

                    case WriteReason.CopyToTail:
                        this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PostSingleWriterCopyToTail, src.Version, default, address);
                        break;

                    case WriteReason.Compaction:
                        this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PostSingleWriterCompaction, src.Version, default, address);
                        break;
                }
                if (!this.isScan)
                {
                    this.cacheTracker.UpdateTrackedObjectSize(key.Val.EstimatedSize + dst.EstimatedSize, key, address);
                }
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.SingleDeleter, null, default, address);
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PostSingleDeleter, null, default, address);
                if (!this.isScan)
                {
                    this.cacheTracker.UpdateTrackedObjectSize(key.Val.EstimatedSize, key, address);
                }
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.ConcurrentWriter(ref Key key, ref EffectTracker input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.ConcurrentWriter, src.Version, default, address);
                if (!this.isScan)
                {
                    this.cacheDebugger?.Fail("Do not expect ConcurrentWriter; all updates are RMW, and SingleWriter is used for CopyToTail", key);
                }
                dst.Val = src.Val;
                dst.Version = src.Version;
                return true;
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.ConcurrentDeleter, value.Version, default, address);
                if (!this.isScan)
                {
                    long removed = value.EstimatedSize;
                    // If record is marked invalid (failed to insert), dispose key as well
                    if (recordInfo.Invalid)
                        removed += key.Val.EstimatedSize;
                    this.cacheTracker.UpdateTrackedObjectSize(-removed, key, address);
                    this.cacheDebugger?.UpdateReferenceValue(ref key.Val, null, 0);
                }
                return true;
            }

            #region Completion Callbacks

            void IFunctions<Key, Value, EffectTracker, Output, object>.ReadCompletionCallback(ref Key key, ref EffectTracker tracker, ref Output output, object context, Status status, RecordMetadata recordMetadata)
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
                            this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.CompletedRead, null, partitionReadEvent.EventIdString, recordMetadata.Address);
                            tracker.ProcessReadResult(partitionReadEvent, key, null);
                            break;

                        case Status.OK:
                            this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.CompletedRead, null, partitionReadEvent.EventIdString, recordMetadata.Address);
                            tracker.ProcessReadResult(partitionReadEvent, key, output.Read(this.store, partitionReadEvent.EventIdString));
                            break;

                        case Status.PENDING:
                            this.partition.ErrorHandler.HandleError("ReadCompletionCallback", "invalid FASTER result code", null, true, false);
                            break;

                        case Status.ERROR:
                            this.partition.ErrorHandler.HandleError("ReadCompletionCallback", "FASTER reported ERROR status", null, true, this.partition.ErrorHandler.IsTerminated);
                            break;
                    }
                }
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.CheckpointCompletionCallback(int sessionId, string sessionName, CommitPoint commitPoint) { }
            void IFunctions<Key, Value, EffectTracker, Output, object>.RMWCompletionCallback(ref Key key, ref EffectTracker input, ref Output output, object ctx, Status status, RecordMetadata recordMetadata) { }
            void IFunctions<Key, Value, EffectTracker, Output, object>.UpsertCompletionCallback(ref Key key, ref EffectTracker input, ref Value value, object ctx) { }
            void IFunctions<Key, Value, EffectTracker, Output, object>.DeleteCompletionCallback(ref Key key, object ctx) { }

            #endregion

 
            void IFunctions<Key, Value, EffectTracker, Output, object>.DisposeKey(ref Key key)
            {
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.DisposeValue(ref Value value)
            {
            }
        }
    }
}
