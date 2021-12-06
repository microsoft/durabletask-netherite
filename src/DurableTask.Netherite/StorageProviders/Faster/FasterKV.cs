// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
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

        TrackedObject[] singletons;
        Task persistSingletonsTask;

        ClientSession<Key, Value, EffectTracker, TrackedObject, object, IFunctions<Key, Value, EffectTracker, TrackedObject, object>> mainSession;

        public FasterKV(Partition partition, BlobManager blobManager)
        {
            this.partition = partition;
            this.blobManager = blobManager;
            this.cacheDebugger = partition.CacheDebugger;

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

            this.terminationToken = partition.ErrorHandler.Token;

            var _ = this.terminationToken.Register(
                () => {
                    try
                    {
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


        ClientSession<Key, Value, EffectTracker, TrackedObject, object, IFunctions<Key, Value, EffectTracker, TrackedObject, object>> CreateASession()
            => this.fht.NewSession<EffectTracker, TrackedObject, object>(new Functions(this.partition, this.StoreStats));

        public override void InitMainSession()
        {
            this.singletons = new TrackedObject[TrackedObjectKey.NumberSingletonTypes];
            this.mainSession = this.CreateASession();
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
                var instanceQuery = queryEvent.InstanceQuery;

                // create an individual session for this query so the main session can be used
                // while the query is progressing.
                using (var session = this.CreateASession())
                {
                    var orchestrationStates = this.ScanOrchestrationStates(effectTracker, queryEvent);

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
                    this.partition.Assert(!key.Val.IsSingleton);
                    TrackedObject target = null;
                    var status = this.mainSession.Read(ref key, ref effectTracker, ref target, readEvent, 0);
                    switch (status)
                    {
                        case Status.NOTFOUND:
                        case Status.OK:
                            // fast path: we hit in the cache and complete the read
                            this.StoreStats.HitCount++;
                            this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.ReadHit, null, readEvent.EventIdString);
                            effectTracker.ProcessReadResult(readEvent, key, target);
                            break;

                        case Status.PENDING:
                            // slow path: read continuation will be called when complete
                            this.StoreStats.MissCount++;
                            this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.ReadMiss, null, readEvent.EventIdString);
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
                    var result = await this.mainSession.ReadAsync(key, effectTracker, context: null, token: this.terminationToken).ConfigureAwait(false);
                    var (status, output) = result.Complete();
                    return output;
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
            ClientSession<Key, Value, EffectTracker, TrackedObject, object, Functions> session,
            Key key,
            EffectTracker effectTracker)
        {
            try
            {
                this.partition.Assert(!key.Val.IsSingleton);
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
                    //var timeoutTask = this.cacheDebugger?.CreateTimer(TimeSpan.FromMinutes(2));
                    
                    var rmwAsyncResultTask = this.mainSession.RMWAsync(ref k, ref tracker, token: this.terminationToken);
                    //await (this.cacheDebugger?.CheckTiming(rmwAsyncResultTask.AsTask(), timeoutTask, $"RMWAsync took too long, suspect hang. key={k}") ?? default);
                    var rmwAsyncResult = await rmwAsyncResultTask;

                    var rmwAsyncCompleteTask = rmwAsyncResult.CompleteAsync();
                    //await (this.cacheDebugger?.CheckTiming(rmwAsyncCompleteTask.AsTask(), timeoutTask, $"RmwAsyncResult.CompleteAsync took too long, suspect hang. key={k}") ?? default);
                    await rmwAsyncCompleteTask;
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
                while (iterator.GetNext(out RecordInfo recordInfo))
                {
                    TrackedObjectKey key = iterator.GetKey().Val;
                    if (!recordInfo.Tombstone)
                    {
                        int version = iterator.GetValue().Version;
                        this.store.cacheDebugger?.Record(ref key, CacheDebugger.CacheEvent.Evict, version, null);
                    }
                    else
                    {
                        this.store.cacheDebugger?.Record(ref key, CacheDebugger.CacheEvent.EvictTombstone, null, null);
                    }
                }
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
                while (iterator.GetNext(out RecordInfo recordInfo))
                {
                    TrackedObjectKey key = iterator.GetKey().Val;
                    if (!recordInfo.Tombstone)
                    {
                        int version = iterator.GetValue().Version;
                        this.store.cacheDebugger?.Record(ref key, CacheDebugger.CacheEvent.Readonly, version, null);
                    }
                    else
                    {
                        this.store.cacheDebugger?.Record(ref key, CacheDebugger.CacheEvent.ReadonlyTombstone, null, null);
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
                    byte[] bytes = this.reader.ReadBytes(count);
                    obj = new Value { Val = bytes, Version = version };
                    if (this.cacheDebugger != null)
                    {
                        var trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                        this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeBytes, version, null);
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
                            this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.SerializeBytes, obj.Version, null);
                        }
                    }
                    else
                    {
                        TrackedObject trackedObject = obj;
                        DurableTask.Netherite.Serializer.SerializeTrackedObject(trackedObject);
                        this.storeStats.Serialize++;
                        this.writer.Write(trackedObject.SerializationCache.Length);
                        this.writer.Write(trackedObject.SerializationCache);
                        this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.SerializeObject, obj.Version, null);
                    }
                }
            }
        }

        public class Functions : IFunctions<Key, Value, EffectTracker, TrackedObject, object>
        {
            readonly Partition partition;
            readonly StoreStatistics stats;
            readonly CacheDebugger cacheDebugger;

            public Functions(Partition partition, StoreStatistics stats)
            {
                this.partition = partition;
                this.stats = stats;
                this.cacheDebugger = partition.CacheDebugger;
            }

            public void InitialUpdater(ref Key key, ref EffectTracker tracker, ref Value value, ref TrackedObject output)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.InitialUpdate, 0, tracker.CurrentEventId);
                this.cacheDebugger?.ConsistentRead(ref key.Val, null, value.Version);
                var trackedObject = TrackedObjectKey.Factory(key.Val);
                this.stats.Create++;
                trackedObject.Partition = this.partition;
                value.Val = trackedObject;
                tracker.ProcessEffectOn(trackedObject);
                value.Version++;
                this.cacheDebugger?.ConsistentWrite(ref key.Val, trackedObject, value.Version);
                this.stats.Modify++;
                this.partition.Assert(value.Val != null);
            }

            public bool InPlaceUpdater(ref Key key, ref EffectTracker tracker, ref Value value, ref TrackedObject output)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.InPlaceUpdate, value.Version, tracker.CurrentEventId);
                if (! (value.Val is TrackedObject trackedObject))
                {
                    var bytes = (byte[])value.Val;
                    this.partition.Assert(bytes != null);
                    trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                    this.stats.Deserialize++;
                    this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, value.Version, tracker.CurrentEventId);
                }
                trackedObject.SerializationCache = null; // cache is invalidated because of update
                trackedObject.Partition = this.partition;
                this.cacheDebugger?.ConsistentRead(ref key.Val, trackedObject, value.Version);
                tracker.ProcessEffectOn(trackedObject);
                value.Version++;
                this.cacheDebugger?.ConsistentWrite(ref key.Val, trackedObject, value.Version);
                this.stats.Modify++;
                this.partition.Assert(value.Val != null);
                return true;
            }

            public bool NeedCopyUpdate(ref Key key, ref EffectTracker tracker, ref Value value, ref TrackedObject output) => true;

            public void CopyUpdater(ref Key key, ref EffectTracker tracker, ref Value oldValue, ref Value newValue, ref TrackedObject output)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.CopyUpdate, oldValue.Version, tracker.CurrentEventId);
                if (oldValue.Val is TrackedObject trackedObject)
                {
                    // replace old object with its serialized snapshot
                    DurableTask.Netherite.Serializer.SerializeTrackedObject(trackedObject);
                    this.stats.Serialize++;
                    oldValue.Val = trackedObject.SerializationCache;
                    this.stats.Copy++;
                }
                else
                {
                    // create new object by deserializing old object
                    var bytes = (byte[])oldValue.Val;
                    this.partition.Assert(bytes != null);
                    trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                    this.stats.Deserialize++;
                    this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, oldValue.Version, tracker.CurrentEventId);
                }

                newValue.Val = trackedObject;
                trackedObject.Partition = this.partition;
                trackedObject.SerializationCache = null; // cache is invalidated by the update which is happening below
                this.cacheDebugger?.ConsistentRead(ref key.Val, trackedObject, oldValue.Version);
                tracker.ProcessEffectOn(trackedObject);
                newValue.Version = oldValue.Version + 1;
                this.cacheDebugger?.ConsistentWrite(ref key.Val, trackedObject, newValue.Version);
                this.stats.Modify++;
                this.partition.Assert(newValue.Val != null);
            }

            public void Reader(ref Key key, ref EffectTracker tracker, ref Value value, ref TrackedObject dst, bool single)
            {
                if (tracker == null)
                {
                    this.cacheDebugger?.Record(key.Val, single ? CacheDebugger.CacheEvent.SingleReaderPrefetch : CacheDebugger.CacheEvent.ConcurrentReaderPrefetch, value.Version, default);

                    // this is a prefetch, so we don't use the value
                    // also, we don't check consistency because it may be stale (as not on the main session)
                    return; 
                }
                else
                {
                    this.cacheDebugger?.Record(key.Val, single ? CacheDebugger.CacheEvent.SingleReader : CacheDebugger.CacheEvent.ConcurrentReader, value.Version, default);

                    TrackedObject trackedObject = null;

                    if (value.Val != null)
                    {
                        if (value.Val is byte[] bytes)
                        {
                            trackedObject = DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes);
                            this.stats.Deserialize++;
                            this.cacheDebugger?.Record(trackedObject.Key, CacheDebugger.CacheEvent.DeserializeObject, value.Version, default);
                        }
                        else
                        {
                            trackedObject = (TrackedObject)value.Val;
                            this.partition.Assert(trackedObject != null);
                        }

                        trackedObject.Partition = this.partition;
                    }

                    dst = trackedObject;
                    this.cacheDebugger?.ConsistentRead(ref key.Val, trackedObject, value.Version);
                    this.stats.Read++;
                }
            }
            public void SingleReader(ref Key key, ref EffectTracker tracker, ref Value value, ref TrackedObject dst)
            {
                this.Reader(ref key, ref tracker, ref value, ref dst, true);
            }
            public void ConcurrentReader(ref Key key, ref EffectTracker tracker, ref Value value, ref TrackedObject dst)
            {
                this.Reader(ref key, ref tracker, ref value, ref dst, false);
            }

            public void SingleWriter(ref Key key, ref Value src, ref Value dst)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.SingleWriter, src.Version, default);
                dst.Val = src.Val;
                dst.Version = src.Version;
            }

            public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.ConcurrentWriter, src.Version, default);
                dst.Val = src.Val;
                dst.Version = src.Version;
                return true;
            }

            public bool ConcurrentDeleter(ref Key key, ref Value src, ref Value dst)
            {
                this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.ConcurrentWriter, src.Version, default);
                dst.Val = src.Val;
                dst.Version = src.Version;
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
                            this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.ReadMissComplete, null, partitionReadEvent.EventIdString);
                            tracker.ProcessReadResult(partitionReadEvent, key, null);
                            break;

                        case Status.OK:
                            this.cacheDebugger?.Record(key.Val, CacheDebugger.CacheEvent.ReadMissComplete, null, partitionReadEvent.EventIdString);
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
            public void RMWCompletionCallback(ref Key key, ref EffectTracker input, ref TrackedObject output, object ctx, Status status) { }
            public void UpsertCompletionCallback(ref Key key, ref Value value, object ctx) { }
            public void DeleteCompletionCallback(ref Key key, object ctx) { }

            // We do not need to lock records, because writes and non-query reads are single-session, and query reads can only race on instance states which are immutable
            public bool SupportsLocking => false;   

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
