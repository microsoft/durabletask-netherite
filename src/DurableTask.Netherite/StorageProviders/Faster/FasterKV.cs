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

    class FasterKV : TrackedObjectStore
    {
        readonly FasterKV<Key, Value> fht;

        readonly Partition partition;
        readonly BlobManager blobManager;
        readonly CancellationToken terminationToken;
        readonly CacheDebugger cacheDebugger;
        readonly MemoryTracker.CacheTracker cacheTracker;

        TrackedObject[] singletons;
        Task persistSingletonsTask;

        ClientSession<Key, Value, EffectTracker, Output, object, IFunctions<Key, Value, EffectTracker, Output, object>> mainSession;

        public FasterKV(Partition partition, BlobManager blobManager, MemoryTracker memoryTracker)
        {
            this.partition = partition;
            this.blobManager = blobManager;
            this.cacheDebugger = partition.Settings.CacheDebugger;
            this.cacheTracker = memoryTracker.NewCacheTracker(this, this.cacheDebugger);

            partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            var storelogsettings = blobManager.GetDefaultStoreLogSettings(partition.Settings.UseSeparatePageBlobStorage, partition.NumberPartitions(), partition.Settings.FasterTuningParameters);

            this.fht = new FasterKV<Key, Value>(
                BlobManager.HashTableSize,
                storelogsettings,
                blobManager.StoreCheckpointSettings,
                new SerializerSettings<Key, Value>
                {
                    keySerializer = () => new Key.Serializer(),
                    valueSerializer = () => new Value.Serializer(this.StoreStats, partition.TraceHelper, this.cacheDebugger),
                });

            this.fht.Log.SubscribeEvictions(new EvictionObserver(this));
            this.fht.Log.Subscribe(new ReadonlyObserver(this));
            partition.Assert(this.fht.ReadCache == null);
            this.cacheTracker.Log = this.fht.Log;

            this.terminationToken = partition.ErrorHandler.Token;

            var _ = this.terminationToken.Register(
                () => {
                    try
                    {
                        this.cacheTracker?.Dispose();
                        this.mainSession?.Dispose();
                        this.fht.Dispose();
                        this.blobManager.HybridLogDevice.Dispose();
                        this.blobManager.ObjectLogDevice.Dispose();
                        this.blobManager.ClosePSFDevices();
                    }
                    catch(Exception e)
                    {
                        this.blobManager.TraceHelper.FasterStorageError("Disposing FasterKV", e);
                    }
                }, 
                useSynchronizationContext: false);

            this.blobManager.TraceHelper.FasterProgress("Constructed FasterKV");
        }

        public void AdjustPageCount(long targetSize, long trackedObjectSize)
        {
            if (this.fht == null)
            {
                return; // this may be called during startup when the store has not been constructed yet
            }

            long totalSize = trackedObjectSize + this.fht.IndexSize * 64 + this.fht.Log.MemorySizeBytes + this.fht.OverflowBucketCount * 64;

            // Adjust empty page count to drive towards desired memory utilization
            if (totalSize > targetSize && this.fht.Log.AllocatableMemorySizeBytes >= this.fht.Log.MemorySizeBytes)
            {
                this.fht.Log.EmptyPageCount++;
            }
            else if (totalSize < targetSize && this.fht.Log.AllocatableMemorySizeBytes <= this.fht.Log.MemorySizeBytes)
            {
                this.fht.Log.EmptyPageCount--;
            }
        }

        ClientSession<Key, Value, EffectTracker, Output, object, IFunctions<Key, Value, EffectTracker, Output, object>> CreateASession(string id)
        {
            var functions = new Functions(this.partition, this, this.cacheTracker);
            return this.fht.NewSession(functions, id);
        }

        public override void InitMainSession()
        {
            this.singletons = new TrackedObject[TrackedObjectKey.NumberSingletonTypes];
            this.mainSession = this.CreateASession("main");
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
                this.mainSession = this.CreateASession("main");

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

        public override bool TakeFullCheckpoint(long commitLogPosition, long inputQueuePosition, out Guid checkpointGuid)
        {
            try
            {
                this.blobManager.CheckpointInfo.CommitLogPosition = commitLogPosition;
                this.blobManager.CheckpointInfo.InputQueuePosition = inputQueuePosition;
                if (this.fht.TakeFullCheckpoint(out checkpointGuid))
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
                if (this.fht.TakeIndexCheckpoint(out var token))
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

        public override Guid? StartStoreCheckpoint(long commitLogPosition, long inputQueuePosition)
        {
            try
            {
                this.blobManager.CheckpointInfo.CommitLogPosition = commitLogPosition;
                this.blobManager.CheckpointInfo.InputQueuePosition = inputQueuePosition;

                if (this.fht.TakeHybridLogCheckpoint(out var token))
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
                using var prefetchSession = this.CreateASession($"prefetch-{Guid.NewGuid():N}");

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
                    this.partition.Assert(!key.Val.IsSingleton);
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
                            this.cacheDebugger?.CheckVersionConsistency(ref key.Val, target, null);
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
        public override async ValueTask<TrackedObject> ReadAsync(Key key, EffectTracker effectTracker)
        {
            try
            {
                if (key.Val.IsSingleton)
                {
                    return this.singletons[(int)key.Val.ObjectType];
                }
                else
                {
                    var result = await this.mainSession.ReadAsync(key, effectTracker, context: null, token: this.terminationToken);
                    var (status, output) = result.Complete();
                    return output.Read(this, null);
                }
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        // read a tracked object on a query session
        async ValueTask<TrackedObject> ReadAsync(
            ClientSession<Key, Value, EffectTracker, Output, object, Functions> session,
            Key key,
            EffectTracker effectTracker)
        {
            try
            {
                this.partition.Assert(!key.Val.IsSingleton);
                var result = await session.ReadAsync(key, effectTracker, context: null, token: this.terminationToken);
                var (status, output) = result.Complete();
                return output.Read(this, "q");
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }


        // create a tracked object on the main session (only one of these is executing at a time)
        public override ValueTask<TrackedObject> CreateAsync(Key key)
        {
            try
            {
                this.partition.Assert(key.Val.IsSingleton);
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
                    int numTries = 3;
                    while (numTries-- > 0)
                    {
                        try
                        {
                            this.cacheDebugger?.Record(k, CacheDebugger.CacheEvent.StartingRMW, null, tracker.CurrentEventId, 0);

                            var rmwAsyncResult = await this.mainSession.RMWAsync(ref k, ref tracker, token: this.terminationToken);

                            this.cacheDebugger?.Record(k, CacheDebugger.CacheEvent.PendingRMW, null, tracker.CurrentEventId, 0);

                            // Synchronous version
                            rmwAsyncResult.Complete();

                            this.cacheDebugger?.Record(k, CacheDebugger.CacheEvent.CompletedRMW, null, tracker.CurrentEventId, 0);

                            break;

                            // As an alternative, can consider the following asynchronous version
                            //{
                            //    this.partition.EventDetailTracer?.TraceEventProcessingDetail($"retrying completion of RMW on {k}");
                            //    rmwAsyncResult = await rmwAsyncResult.CompleteAsync();
                            //}
                            //while (rmwAsyncResult.Status == Status.PENDING)
                            //}
                        }
                        catch (Exception exception) when (!Utils.IsFatal(exception))
                        {
                            if (numTries > 0)
                            {
                                await Task.Yield();
                                continue;
                            }
                            else
                            {
                                this.cacheDebugger.Fail($"Failed to execute RMW in Faster: {exception}", k);
                                throw;
                            }
                        }
                    }
                }
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
                using var session = this.CreateASession($"scan-{Guid.NewGuid():N}");

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
                                instanceState = (InstanceState)val;
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

        public override void CheckInvariants()
        {
            if (this.cacheDebugger != null)
            {
                //this.ValidateMemoryTracker();
            }
        }

        public void ValidateMemoryTracker()
        {
            if (this.cacheDebugger == null)
            {
                return; // we only do this when the cache debugger is attached
            }

            var inMemoryIterator = this.fht.Log.Scan(this.fht.Log.HeadAddress, this.fht.Log.TailAddress);

            long totalSize = 0;
            Dictionary<TrackedObjectKey, (long Size, StringBuilder Sb)> perKeySize = new Dictionary<TrackedObjectKey, (long Size, StringBuilder Sb)>(); 
            void Add(TrackedObjectKey key, long delta, string desc)
            {
                perKeySize.TryGetValue(key, out (long size, StringBuilder sb) current);
                current.size += delta;
                if (current.sb == null)
                {
                    current.sb = new StringBuilder();
                }
                else
                {
                    current.sb.Append(',');
                }
                current.sb.Append(desc);
                perKeySize[key] = current;
                totalSize += delta;
            }
            
            while (inMemoryIterator.GetNext(out RecordInfo recordInfo, out Key key, out Value value))
            {
                long delta = key.Val.EstimatedSize;
                if (!recordInfo.Tombstone)
                {
                   delta += value.ComputeEstimatedSize();
                }
                Add(key, delta, $"{(recordInfo.Invalid?"I":"")}{(recordInfo.Tombstone ? "T" : "")}{delta}@{inMemoryIterator.CurrentAddress.ToString("x")}");
            }

            long trackedSize = this.cacheTracker.TrackedObjectSize;

            if (trackedSize != totalSize)
            {
                foreach (var kvp in perKeySize)
                {
                    if (!this.cacheDebugger.CheckSize(kvp.Key, kvp.Value.Size, kvp.Value.Sb.ToString()))
                    {

                        break;
                    }
                }        
            }
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
                long sumOfDeltas = 0;

                while (iterator.GetNext(out RecordInfo recordInfo, out Key key, out Value value))
                {
                    long delta;
                    if (!recordInfo.Tombstone)
                    {
                        this.store.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.Evict, value.Version, null, iterator.CurrentAddress);
                        delta = key.Val.EstimatedSize + value.ComputeEstimatedSize();
                    }
                    else
                    {
                        this.store.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.EvictTombstone, null, null, iterator.CurrentAddress);
                        delta = key.Val.EstimatedSize;
                    }

                    sumOfDeltas += delta;
                }

                //this.store.cacheTracker.UpdateTrackedObjectSize(-sumOfDeltas);
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

            public int Version; // for debugging FASTER

            public static implicit operator TrackedObject(Value v) => (TrackedObject)v.Val;
            public static implicit operator Value(TrackedObject v) => new Value() { Val = v };

            public override string ToString() => this.Val.ToString();

            public long ComputeEstimatedSize() => 4 + (
                this.Val is byte[] bytes ? 4 + bytes.Length :
                this.Val is TrackedObject o ? o.ComputeEstimatedSize() :
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
                        TrackedObject trackedObject = obj;
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
            //public long EstimatedSize;

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
                    store.partition.Assert(bytes != null);
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

            public Functions(Partition partition, FasterKV store, MemoryTracker.CacheTracker cacheTracker)
            {
                this.partition = partition;
                this.store = store;
                this.stats = store.StoreStats;
                this.cacheDebugger = partition.Settings.CacheDebugger;
                this.cacheTracker = cacheTracker;
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.SupportsPostOperations 
                => true;

            bool IFunctions<Key, Value, EffectTracker, Output, object>.NeedInitialUpdate(ref Key key, ref EffectTracker input, ref Output output)
                => true;

            void IFunctions<Key, Value, EffectTracker, Output, object>.InitialUpdater(ref Key key, ref EffectTracker tracker, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.InitialUpdate, 0, tracker.CurrentEventId, address);
                this.cacheDebugger?.ValidateObjectVersion(value, key.Val);
                this.cacheDebugger?.CheckVersionConsistency(ref key.Val, null, value.Version);
                var trackedObject = TrackedObjectKey.Factory(key.Val);
                this.stats.Create++;
                trackedObject.Partition = this.partition;
                value.Val = trackedObject;
                tracker.ProcessEffectOn(trackedObject);
                value.Version++;
                this.cacheDebugger?.UpdateReferenceValue(ref key.Val, trackedObject, value.Version);
                this.stats.Modify++;
                this.partition.Assert(value.Val != null);
                this.cacheDebugger?.ValidateObjectVersion(value, key.Val);
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.PostInitialUpdater(ref Key key, ref EffectTracker tracker, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PostInitialUpdate, value.Version, tracker.CurrentEventId, address);
                // we have inserted a new entry at the tail
                //this.cacheTracker?.UpdateTrackedObjectSize(key.Val.EstimatedSize + value.ComputeEstimatedSize(), key, address);
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.InPlaceUpdater(ref Key key, ref EffectTracker tracker, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.InPlaceUpdate, value.Version, tracker.CurrentEventId, address);
                this.cacheDebugger?.ValidateObjectVersion(value, key.Val);
                //long sizeBeforeUpdate = value.ComputeEstimatedSize();
                if (! (value.Val is TrackedObject trackedObject))
                {
                    var bytes = (byte[])value.Val;
                    this.partition.Assert(bytes != null);
                    trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                    this.stats.Deserialize++;
                    value.Val = trackedObject;
                    this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, value.Version, tracker.CurrentEventId, 0);
                }
                trackedObject.SerializationCache = null; // cache is invalidated because of update
                trackedObject.Partition = this.partition;
                this.cacheDebugger?.CheckVersionConsistency(ref key.Val, trackedObject, value.Version);
                tracker.ProcessEffectOn(trackedObject);
                value.Version++;
                //long sizeAfterUpdate = value.ComputeEstimatedSize();
                this.cacheDebugger?.UpdateReferenceValue(ref key.Val, trackedObject, value.Version);
                this.stats.Modify++;
                this.partition.Assert(value.Val != null);
                //this.cacheTracker?.UpdateTrackedObjectSize(sizeAfterUpdate - sizeBeforeUpdate, key, address);
                this.cacheDebugger?.ValidateObjectVersion(value, key.Val);
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
                    //long oldValueSizeBefore = oldValue.ComputeEstimatedSize();
                    DurableTask.Netherite.Serializer.SerializeTrackedObject(trackedObject);
                    this.stats.Serialize++;
                    this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.SerializeObject, oldValue.Version, null, 0);
                    oldValue.Val = trackedObject.SerializationCache;
                    //long oldValueSizeAfter = oldValue.ComputeEstimatedSize();
                    //this.cacheTracker?.UpdateTrackedObjectSize(oldValueSizeAfter - oldValueSizeBefore, key, address);
                    this.stats.Copy++;
                }
                else
                {
                    // create new object by deserializing old object
                    var bytes = (byte[])oldValue.Val;
                    this.partition.Assert(bytes != null);
                    trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                    this.stats.Deserialize++;
                    trackedObject.Partition = this.partition;
                    this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, oldValue.Version, tracker.CurrentEventId, 0);
                }

                newValue.Val = trackedObject;
                trackedObject.SerializationCache = null; // cache is invalidated by the update which is happening below
                this.cacheDebugger?.CheckVersionConsistency(ref key.Val, trackedObject, oldValue.Version);
                tracker.ProcessEffectOn(trackedObject);
                newValue.Version = oldValue.Version + 1;
                this.cacheDebugger?.UpdateReferenceValue(ref key.Val, trackedObject, newValue.Version);
                this.stats.Modify++;
                this.partition.Assert(newValue.Val != null);
                this.cacheDebugger?.ValidateObjectVersion(oldValue, key.Val);
                this.cacheDebugger?.ValidateObjectVersion(newValue, key.Val);
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.PostCopyUpdater(ref Key key, ref EffectTracker tracker, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PostCopyUpdate, newValue.Version, tracker.CurrentEventId, address);
                //this.cacheTracker?.UpdateTrackedObjectSize(key.Val.EstimatedSize + newValue.ComputeEstimatedSize(), key, address);
                return true;
            }

            bool Reader(ref Key key, ref EffectTracker tracker, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address, bool single)
            {
                this.cacheDebugger?.Record(key.Val, single ? CacheDebugger.CacheEvent.SingleReader : CacheDebugger.CacheEvent.ConcurrentReader, value.Version, default, address);
                this.cacheDebugger?.ValidateObjectVersion(value, key.Val);

                TrackedObject trackedObject = null;

                if (value.Val != null)
                {
                    if (value.Val is byte[] bytes)
                    {
                        if (!single)
                        {
                            this.cacheDebugger?.Fail("Unexpected byte[] state in mutable section");
                        }
                        trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                        this.stats.Deserialize++;
                        trackedObject.Partition = this.partition;
                        this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, value.Version, default, 0);
                    }
                    else
                    {
                        trackedObject = (TrackedObject)value.Val;
                        this.partition.Assert(trackedObject != null);

                        if (single)
                        {
                            // replace old object with its serialized snapshot
                            //long valueSizeBefore = dst.EstimatedSize = value.ComputeEstimatedSize();
                            DurableTask.Netherite.Serializer.SerializeTrackedObject(trackedObject);
                            this.stats.Serialize++;
                            this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.SerializeObject, value.Version, null, 0);
                            value.Val = trackedObject.SerializationCache;
                            //long valueSizeAfter = value.ComputeEstimatedSize();
                            //this.cacheTracker?.UpdateTrackedObjectSize(valueSizeAfter - valueSizeBefore, key, address);
                            this.stats.Copy++;
                        }
                    }
                }

                dst.Val = trackedObject;
                this.stats.Read++;
                return true;
            }
            bool IFunctions<Key, Value, EffectTracker, Output, object>.SingleReader(ref Key key, ref EffectTracker tracker, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address)
            {
                return this.Reader(ref key, ref tracker, ref value, ref dst, ref recordInfo, address, true);
            }
            bool IFunctions<Key, Value, EffectTracker, Output, object>.ConcurrentReader(ref Key key, ref EffectTracker tracker, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address)
            {
                return this.Reader(ref key, ref tracker, ref value, ref dst, ref recordInfo, address, false);
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.SingleWriter(ref Key key, ref EffectTracker input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address)
            {
                // This is called when a read copies the value to the tail. 

                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.SingleWriter, src.Version, default, address);
                this.cacheDebugger?.ValidateObjectVersion(src, key.Val);

                if (output.Val != null)
                {
                    // this is called after SingleReader and has a tracked object ready to use
                    dst.Val = output.Val;
                }
                else
                {
                    if (src.Val == null)
                    {
                        dst.Val = src.Val;
                    }
                    else if (src.Val is byte[] bytes)
                    {
                        var trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                        this.stats.Deserialize++;
                        this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, src.Version, null, 0);
                        trackedObject.Partition = this.partition;
                        dst.Val = trackedObject;
                    }
                    else
                    {
                        this.cacheDebugger?.Fail("SingleWriter should not be called with mutable source", key);
                    }
                }
                dst.Version = src.Version;

                // compute the estimated size, or use the cached value from the SingleRead
                //long estimatedSize = (output.EstimatedSize > 0) ? output.EstimatedSize : dst.ComputeEstimatedSize();
                //this.cacheTracker?.UpdateTrackedObjectSize(key.Val.EstimatedSize + estimatedSize, key, address);

                this.cacheDebugger?.ValidateObjectVersion(dst, key.Val);
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.PostSingleWriter(ref Key key, ref EffectTracker input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PostSingleWriter, src.Version, default, address);

                if (input != null)
                    this.cacheDebugger?.Fail("PostSingleWriter should only be called by queries", key);
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.PostSingleDeleter, null, default, address);
                //this.cacheTracker?.UpdateTrackedObjectSize(key.Val.EstimatedSize, key, address);
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.ConcurrentWriter(ref Key key, ref EffectTracker input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.ConcurrentWriter, src.Version, default, address);
                this.cacheDebugger?.Fail("Do not expect ConcurrentWriter; all updates are RMW, and SingleWriter is used for CopyToTail", key);
                //long sizeBeforeUpdate = dst.ComputeEstimatedSize();
                dst.Val = src.Val;
                dst.Version = src.Version;
                //long sizeAfterUpdate = dst.ComputeEstimatedSize();
                //this.cacheTracker?.UpdateTrackedObjectSize(sizeAfterUpdate - sizeBeforeUpdate, key, address);
                return true;
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.ConcurrentDeleter, value.Version, default, address);
                long removed = value.ComputeEstimatedSize();

                // If record is marked invalid (failed to insert), dispose key as well
                if (recordInfo.Invalid)
                    removed += key.Val.EstimatedSize;

                //this.cacheTracker?.UpdateTrackedObjectSize(-removed, key, address);
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

            void IFunctions<Key, Value, EffectTracker, Output, object>.CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
            void IFunctions<Key, Value, EffectTracker, Output, object>.RMWCompletionCallback(ref Key key, ref EffectTracker input, ref Output output, object ctx, Status status, RecordMetadata recordMetadata) { }
            void IFunctions<Key, Value, EffectTracker, Output, object>.UpsertCompletionCallback(ref Key key, ref EffectTracker input, ref Value value, object ctx) { }
            void IFunctions<Key, Value, EffectTracker, Output, object>.DeleteCompletionCallback(ref Key key, object ctx) { }

            #endregion

            #region Locking

            // We do not need to lock records, because writes and non-query reads are single-session, and query reads can only race on instance states which are immutable

            bool IFunctions<Key, Value, EffectTracker, Output, object>.SupportsLocking
                => false;

            void IFunctions<Key, Value, EffectTracker, Output, object>.LockExclusive(ref RecordInfo recordInfo, ref Key key, ref Value value, ref long lockContext)
            {
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.UnlockExclusive(ref RecordInfo recordInfo, ref Key key, ref Value value, long lockContext)
            {
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.TryLockExclusive(ref RecordInfo recordInfo, ref Key key, ref Value value, ref long lockContext, int spinCount)
            {
                return true;
            }

            void IFunctions<Key, Value, EffectTracker, Output, object>.LockShared(ref RecordInfo recordInfo, ref Key key, ref Value value, ref long lockContext)
            {
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.UnlockShared(ref RecordInfo recordInfo, ref Key key, ref Value value, long lockContext)
            {
                return true;
            }

            bool IFunctions<Key, Value, EffectTracker, Output, object>.TryLockShared(ref RecordInfo recordInfo, ref Key key, ref Value value, ref long lockContext, int spinCount)
            {
                return true;
            }

            #endregion
        }
    }
}
