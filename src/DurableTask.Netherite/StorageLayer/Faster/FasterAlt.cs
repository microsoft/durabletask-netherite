// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Blobs.Specialized;
    using DurableTask.Core.Common;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// An alternative implementation of the persistent cache
    /// </summary>
    class FasterAlt : TrackedObjectStore
    {
        readonly Partition partition;
        readonly BlobManager blobManager;
        readonly CancellationToken terminationToken;
        readonly string prefix;
        readonly FasterTraceHelper traceHelper;
        readonly FasterTraceHelper detailTracer;

        // the cache containing all the TrackedObjects currently in memory
        readonly Dictionary<TrackedObjectKey, CacheEntry> cache = new Dictionary<TrackedObjectKey, CacheEntry>();

        // the cache entries that are in the process of being loaded into memory
        readonly Dictionary<TrackedObjectKey, PendingLoad> pendingLoads = new Dictionary<TrackedObjectKey, PendingLoad>();

        // the cache entries that have been modified relative to storage
        readonly List<CacheEntry> modified = new List<CacheEntry>();

        // the list of checkpoints whose writes should be ignored when loading objects
        readonly HashSet<Guid> failedCheckpoints = new HashSet<Guid>();

        // the checkpoint currently in progress
        Task checkpointTask;

        class CacheEntry
        {
            public byte[] LastCheckpointed;
            public TrackedObject TrackedObject;
            public bool Modified;
        }

        struct ToWrite
        {
            public TrackedObjectKey Key;
            public byte[] PreviousValue;
            public byte[] NewValue;
        }

        struct ToRead
        {
            public byte[] PreviousValue;
            public byte[] NewValue;
            public Guid Guid;
        }

        class PendingLoad
        {
            public Task<ToRead> LoadTask;
            public EffectTracker EffectTracker;
            public List<PartitionReadEvent> ReadEvents;
        }

        public FasterAlt(Partition partition, BlobManager blobManager)
        {
            this.partition = partition;
            this.blobManager = blobManager;
            this.prefix = $"p{this.partition.PartitionId:D2}/store/";

            this.terminationToken = partition.ErrorHandler.Token;
            this.traceHelper = blobManager.TraceHelper;
            this.detailTracer = this.traceHelper.IsTracingAtMostDetailedLevel ? this.traceHelper : null;

            var _ = this.terminationToken.Register(
                () => {
                    // nothing so far
                },
                useSynchronizationContext: false);

            this.blobManager.TraceHelper.FasterProgress("Constructed FasterAlt");
        }

        public override (double totalSizeMB, int fillPercentage) CacheSizeInfo => (0.0,0);

        public override void InitMainSession()
        {
        }

        public override void AdjustCacheSize()
        {
        }

        public override Task<bool> FindCheckpointAsync(bool logIsEmpty)
        {
            return Task.FromResult(!logIsEmpty);
        }

        public override Task<(long commitLogPosition, long inputQueuePosition, string inputQueueFingerprint)> RecoverAsync()
        {
            foreach (var guid in this.ReadCheckpointIntentions())
            {
                this.failedCheckpoints.Add(guid);
            }

            var tasks = new List<Task>();

            // kick off loads for all singletons
            foreach (var key in TrackedObjectKey.GetSingletons())
            {
                var loadTask = this.LoadAsync(key);
                this.pendingLoads.Add(key, new PendingLoad()
                {
                    EffectTracker = null,
                    ReadEvents = new List<PartitionReadEvent>(),
                    LoadTask = loadTask,
                });
                tasks.Add(loadTask);
            }

            Task.WhenAll(tasks).GetAwaiter().GetResult();

            this.CompletePending();

            var dedupState = (DedupState)this.cache[TrackedObjectKey.Dedup].TrackedObject;
            return Task.FromResult((dedupState.Positions.Item1, dedupState.Positions.Item2, "TODO"));
        }

        public override bool CompletePending()
        {
            var completed = this.pendingLoads.Where(p => p.Value.LoadTask.IsCompleted).ToList();

            foreach (var kvp in completed)
            {
                this.ProcessCompletedLoad(kvp.Key, kvp.Value);
            }

            return this.pendingLoads.Count == 0;
        }

        public override ValueTask ReadyToCompletePendingAsync(CancellationToken token)
        {
            if (this.pendingLoads.Count == 0)
            {
                return default;
            }
            else
            {
                return new ValueTask(Task.WhenAny(this.pendingLoads.Select(kvp => kvp.Value.LoadTask)));
            }
        }

        public override bool TakeFullCheckpoint(long commitLogPosition, long inputQueuePosition, string inputQueueFingerprint, out Guid checkpointGuid)
        {
            checkpointGuid = Guid.NewGuid();
            this.StartStoreCheckpoint(commitLogPosition, inputQueuePosition, checkpointGuid);
            return true;
        }

        public override Task RemoveObsoleteCheckpoints()
        {
            //TODO
            return Task.CompletedTask;
        }

        public async override ValueTask CompleteCheckpointAsync()
        {
            await this.checkpointTask.ConfigureAwait(false);
        }

        public override Guid? StartIndexCheckpoint()
        {
            this.checkpointTask = Task.CompletedTask; // this implementation does not contain an index (yet).
            return default;
        }

        public override Guid? StartStoreCheckpoint(long commitLogPosition, long inputQueuePosition, string inputQueueFingerprint, long? shiftBeginAddress)
        {
            var guid = Guid.NewGuid();
            this.StartStoreCheckpoint(commitLogPosition, inputQueuePosition, guid);
            return guid;
        }

        internal void StartStoreCheckpoint(long commitLogPosition, long inputQueuePosition, Guid guid)
        {
            // update the positions
            var dedupState = this.cache[TrackedObjectKey.Dedup];
            ((DedupState)dedupState.TrackedObject).Positions = (commitLogPosition, inputQueuePosition);
            if (!dedupState.Modified)
            {
                dedupState.Modified = true;
                this.modified.Add(dedupState);
            }

            // figure out which objects need to be written back
            var toWrite = new List<ToWrite>();
            foreach (var cacheEntry in this.modified)
            {
                byte[] bytes = Serializer.SerializeTrackedObject(cacheEntry.TrackedObject);
                toWrite.Add(new ToWrite()
                {
                    Key = cacheEntry.TrackedObject.Key,
                    PreviousValue = cacheEntry.LastCheckpointed,
                    NewValue = bytes,
                });
                cacheEntry.LastCheckpointed = bytes;
                cacheEntry.Modified = false;
            }
            this.modified.Clear();

            this.checkpointTask = Task.Run(() => this.WriteCheckpointAsync(toWrite, guid));
        }

        async Task WriteCheckpointAsync(List<ToWrite> toWrite, Guid guid)
        {
            // the intention file instructs subsequent recoveries to ignore updates should we fail in the middle
            await this.WriteCheckpointIntention(guid).ConfigureAwait(false);

            var guidbytes = guid.ToByteArray();
            var tasks = new List<Task>();
            foreach (var entry in toWrite)
            {
                tasks.Add(this.StoreAsync(guidbytes, entry));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        public override Task FinalizeCheckpointCompletedAsync(Guid guid)
        {
            // we have finished the checkpoint; it is committed by removing the intention file
            return this.RemoveCheckpointIntention(guid);
        }

        public override void CheckInvariants()
        {
        }


        // perform a query
        public override Task QueryAsync(PartitionQueryEvent queryEvent, EffectTracker effectTracker)
        {
            // TODO
            throw new NotImplementedException();
        }

        // kick off a read of a tracked object, completing asynchronously if necessary
        public override void Read(PartitionReadEvent readEvent, EffectTracker effectTracker)
        {
            if (readEvent.Prefetch.HasValue)
            {
                TryRead(readEvent.Prefetch.Value);
            }

            TryRead(readEvent.ReadTarget);

            void TryRead(TrackedObjectKey key)
            {
                if (this.cache.TryGetValue(key, out var entry))
                {
                    this.StoreStats.HitCount++;
                    effectTracker.ProcessReadResult(readEvent, key, entry.TrackedObject);
                }
                else if (this.pendingLoads.TryGetValue(key, out var pendingLoad))
                {
                    this.StoreStats.HitCount++;
                    pendingLoad.EffectTracker = effectTracker;
                    pendingLoad.ReadEvents.Add(readEvent);
                }
                else
                {
                    this.StoreStats.MissCount++;
                    this.pendingLoads.Add(key, new PendingLoad()
                    {
                        EffectTracker = effectTracker,
                        ReadEvents = new List<PartitionReadEvent>() { readEvent },
                        LoadTask = this.LoadAsync(key),
                    });
                }
            }
        }

        // read a tracked object on the main session and wait for the response (only one of these is executing at a time)
        public override async ValueTask<TrackedObject> ReadAsync(FasterKV.Key key, EffectTracker effectTracker)
        {
            if (this.cache.TryGetValue(key.Val, out var entry))
            {
                this.StoreStats.HitCount++;
                return entry.TrackedObject;
            }
            else if (this.pendingLoads.TryGetValue(key, out var pendingLoad))
            {
                this.StoreStats.HitCount++;
                await pendingLoad.LoadTask.ConfigureAwait(false);
                return this.ProcessCompletedLoad(key, pendingLoad);
            }
            else
            {
                this.StoreStats.MissCount++;
                this.pendingLoads.Add(key, pendingLoad = new PendingLoad()
                {
                    EffectTracker = effectTracker,
                    ReadEvents = new List<PartitionReadEvent>(),
                    LoadTask = this.LoadAsync(key),
                });
                await pendingLoad.LoadTask.ConfigureAwait(false);
                return this.ProcessCompletedLoad(key, pendingLoad);
            }
        }

        CacheEntry ProcessStorageRecord(TrackedObjectKey key, ToRead toRead)
        {
            byte[] bytes;
            if (!this.failedCheckpoints.Contains(toRead.Guid))
            {
                bytes = toRead.NewValue;
            }
            else
            {
                bytes = toRead.PreviousValue;
            }
            var trackedObject = (bytes == null) ? TrackedObjectKey.Factory(key) : Serializer.DeserializeTrackedObject(bytes);
            trackedObject.Partition = this.partition;

            return new CacheEntry()
            {
                LastCheckpointed = bytes,
                TrackedObject = trackedObject,
            };
        }

        TrackedObject ProcessCompletedLoad(TrackedObjectKey key, PendingLoad pendingLoad)
        {
            var cacheEntry = this.ProcessStorageRecord(key, pendingLoad.LoadTask.Result);
        
            // install in cache
            this.cache.Add(key, cacheEntry);

            // process the read events that were waiting
            foreach (var evt in pendingLoad.ReadEvents)
            {
                pendingLoad.EffectTracker.ProcessReadResult(evt, key, cacheEntry.TrackedObject);
            }

            // remove from dictionary
            this.pendingLoads.Remove(key);

            return cacheEntry.TrackedObject;
        }

        // create a tracked object on the main session (only one of these is executing at a time)
        public override ValueTask<TrackedObject> CreateAsync(FasterKV.Key key)
        {
            var trackedObject = TrackedObjectKey.Factory(key.Val);
            trackedObject.Partition = this.partition;
            var cacheEntry = new CacheEntry()
            {
                LastCheckpointed = null,
                Modified = true,
                TrackedObject = trackedObject,
            };
            this.cache.Add(key, cacheEntry);
            this.modified.Add(cacheEntry);
            return new ValueTask<TrackedObject>(trackedObject);
        }

        public override async ValueTask ProcessEffectOnTrackedObject(FasterKV.Key key, EffectTracker effectTracker)
        {
            if (!this.cache.TryGetValue(key, out var cacheEntry))
            {
                this.partition.Assert(!this.pendingLoads.ContainsKey(key), "key already there in FasterAlt");
                var storageRecord = await this.LoadAsync(key);
                cacheEntry = this.ProcessStorageRecord(key, storageRecord);
                this.cache.Add(key, cacheEntry);
            }
            var trackedObject = cacheEntry.TrackedObject;
            effectTracker.ProcessEffectOn(trackedObject);
            if (!cacheEntry.Modified)
            {
                cacheEntry.Modified = true;
                this.modified.Add(cacheEntry);
            }
        }

        public override ValueTask RemoveKeys(IEnumerable<TrackedObjectKey> keys)
        {
            // TODO
            throw new NotImplementedException();
        }

        public override void EmitCurrentState(Action<TrackedObjectKey, TrackedObject> emitItem)
        {
            // TODO
            throw new NotImplementedException();
        }


        #region storage access operation

        BlobUtilsV12.BlockBlobClients GetBlob(TrackedObjectKey key)
        {
            StringBuilder blobName = new StringBuilder(this.prefix);
            blobName.Append(key.ObjectType.ToString());
            if (!key.IsSingleton)
            {
                blobName.Append('/');
                blobName.Append(key.InstanceId);
            }
            // TODO validate blob name and handle problems (too long, too many slashes)
            return BlobUtilsV12.GetBlockBlobClients(this.blobManager.BlockBlobContainer, blobName.ToString());
        }

        async Task<ToRead> LoadAsync(TrackedObjectKey key)
        {
            this.detailTracer?.FasterStorageProgress($"StorageOpCalled FasterAlt.LoadAsync key={key}");

            try
            {
                await BlobManager.AsynchronousStorageReadMaxConcurrency.WaitAsync();

                int numAttempts = 0;
                var blob = this.GetBlob(key);

                while (true) // retry loop
                {
                    numAttempts++;
                    try
                    {
                        Interlocked.Increment(ref this.blobManager.LeaseUsers);
                        await this.blobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);

                        using var stream = new MemoryStream();
                        this.detailTracer?.FasterStorageProgress($"starting download target={blob.Name} attempt={numAttempts}");
                        using var response = await blob.WithRetries.DownloadToAsync(stream, cancellationToken: this.blobManager.PartitionErrorHandler.Token).ConfigureAwait(false);
                        this.detailTracer?.FasterStorageProgress($"finished download target={blob.Name} readLength={stream.Position}");

                        // parse the content and return it
                        stream.Seek(0, SeekOrigin.Begin);
                        using var reader = new BinaryReader(stream, Encoding.UTF8);
                        var toRead = new ToRead();
                        var previousLength = reader.ReadInt32();
                        toRead.PreviousValue = previousLength > 0 ? reader.ReadBytes(previousLength) : null;
                        var newLength = reader.ReadInt32();
                        toRead.NewValue = newLength > 0 ? reader.ReadBytes(newLength) : null;
                        toRead.Guid = new Guid(reader.ReadBytes(16));

                        this.detailTracer?.FasterStorageProgress($"StorageOpReturned FasterAlt.LoadAsync key={key}");
                        return toRead;
                    }
                    catch (Azure.RequestFailedException) when (this.terminationToken.IsCancellationRequested)
                    {
                        throw new OperationCanceledException("Partition was terminated.", this.terminationToken);
                    }
                    catch (Azure.RequestFailedException e) when (BlobUtils.IsTransientStorageError(e) && !this.terminationToken.IsCancellationRequested && numAttempts < BlobManager.MaxRetries)
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.blobManager?.HandleStorageError(nameof(LoadAsync), $"Could not read object from storage, will retry in {nextRetryIn}s, numAttempts={numAttempts}", blob.Name, e, false, true);
                        await Task.Delay(nextRetryIn);
                        continue;
                    }
                    catch (TaskCanceledException e) when (BlobUtils.IsTimeout(e) && numAttempts < BlobManager.MaxRetries)
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.blobManager?.HandleStorageError(nameof(LoadAsync), $"Could not read object from storage, will retry in {nextRetryIn}s, numAttempts={numAttempts}", blob.Name, e, false, true);
                        await Task.Delay(nextRetryIn);
                        continue;
                    }
                    catch (Exception exception) when (!Utils.IsFatal(exception))
                    {
                        this.blobManager.PartitionErrorHandler.HandleError(nameof(LoadAsync), "Could not read object from storage", exception, true, this.blobManager.PartitionErrorHandler.IsTerminated);
                        throw;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref this.blobManager.LeaseUsers);
                    }
                };
            }
            finally
            {
                BlobManager.AsynchronousStorageReadMaxConcurrency.Release();
            }
        }

        async Task StoreAsync(byte[] guid, ToWrite entry)
        {
            this.detailTracer?.FasterStorageProgress($"StorageOpCalled FasterAlt.LoadAsync {entry.Key}");

            // assemble the bytes to write
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream, Encoding.UTF8);
            if (entry.PreviousValue == null)
            {
                writer.Write(0);
            }
            else
            {
                writer.Write(entry.PreviousValue.Length);
                writer.Write(entry.PreviousValue);
            }
            if (entry.NewValue == null)
            {
                writer.Write(0);
            }
            else
            {
                writer.Write(entry.NewValue.Length);
                writer.Write(entry.NewValue);
            }
            writer.Write(guid);
            writer.Flush();
            long length = stream.Position;
            stream.Seek(0, SeekOrigin.Begin);

            try
            {
                await BlobManager.AsynchronousStorageWriteMaxConcurrency.WaitAsync();

                int numAttempts = 0;
                var blob = this.GetBlob(entry.Key);

                while (true) // retry loop
                {
                    numAttempts++;
                    try
                    {
                        Interlocked.Increment(ref this.blobManager.LeaseUsers);

                        await this.blobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);

                        this.detailTracer?.FasterStorageProgress($"starting upload target={blob.Name} length={length} attempt={numAttempts}");

                        var client = numAttempts > 1 ? blob.Default : blob.Aggressive;

                        await client.UploadAsync(stream, cancellationToken: this.blobManager.PartitionErrorHandler.Token).ConfigureAwait(false);

                        this.detailTracer?.FasterStorageProgress($"finished upload target={blob.Name} length={length}");
                        return;
                    }
                    catch (Azure.RequestFailedException) when (this.terminationToken.IsCancellationRequested)
                    {
                        throw new OperationCanceledException("Partition was terminated.", this.terminationToken);
                    }
                    catch (Exception e) when (BlobUtils.IsTransientStorageError(e) && !this.terminationToken.IsCancellationRequested && numAttempts < BlobManager.MaxRetries)
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.blobManager?.HandleStorageError(nameof(StoreAsync), $"could not write object to storage, will retry in {nextRetryIn}s, numAttempts={numAttempts}", blob.Name, e, false, true);
                        await Task.Delay(nextRetryIn);
                        continue;
                    }
                    catch (TaskCanceledException e) when (BlobUtils.IsTimeout(e) && numAttempts < BlobManager.MaxRetries)
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.blobManager?.HandleStorageError(nameof(StoreAsync), $"could not write object to storage, will retry in {nextRetryIn}s, numAttempts={numAttempts}", blob.Name, e, false, true);
                        await Task.Delay(nextRetryIn);
                        continue;
                    }
                    catch (Exception exception) when (!Utils.IsFatal(exception))
                    {
                        this.blobManager?.HandleStorageError(nameof(StoreAsync), "could not write object to storage", blob.Name, exception, true, this.blobManager.PartitionErrorHandler.IsTerminated);
                        throw;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref this.blobManager.LeaseUsers);
                    }
                }
            }
            finally
            {
                BlobManager.AsynchronousStorageWriteMaxConcurrency.Release();
            }
        }

        async Task WriteCheckpointIntention(Guid guid)
        {
            try
            {
                Interlocked.Increment(ref this.blobManager.LeaseUsers);
                var blob = BlobUtilsV12.GetBlockBlobClients(this.blobManager.BlockBlobContainer, $"p{this.partition.PartitionId:D2}/incomplete-checkpoints/{guid}");
                await this.blobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                using var response = await blob.WithRetries.DeleteAsync(cancellationToken: this.blobManager.PartitionErrorHandler.Token);
            }
            catch (Azure.RequestFailedException) when (this.terminationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException("Partition was terminated.", this.terminationToken);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.blobManager.PartitionErrorHandler.HandleError(nameof(WriteCheckpointIntention), "Failed to write checkpoint intention to storage", e, true, this.blobManager.PartitionErrorHandler.IsTerminated);
                throw;
            }
            finally
            {
                Interlocked.Decrement(ref this.blobManager.LeaseUsers);
            }
        }

        async Task RemoveCheckpointIntention(Guid guid)
        {
            try
            {
                Interlocked.Increment(ref this.blobManager.LeaseUsers);
                var blob = BlobUtilsV12.GetBlockBlobClients(this.blobManager.BlockBlobContainer, $"p{this.partition.PartitionId:D2}/incomplete-checkpoints/{guid}");
                await this.blobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                using var response = await blob.WithRetries.DeleteAsync(cancellationToken: this.blobManager.PartitionErrorHandler.Token);
            }
            catch (Azure.RequestFailedException) when (this.terminationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException("Partition was terminated.", this.terminationToken);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.blobManager.PartitionErrorHandler.HandleError(nameof(RemoveCheckpointIntention), "Failed to remove checkpoint intention from storage", e, true, false);
                throw;
            }
            finally
            {
                Interlocked.Decrement(ref this.blobManager.LeaseUsers);
            }
        }

        static readonly int guidLength = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".Length;


        IEnumerable<Guid> ReadCheckpointIntentions()
        {
            try
            {
                string prefix = $"p{this.partition.PartitionId:D2}/incomplete-checkpoints/";
                var checkPoints = this.blobManager.BlockBlobContainer.WithRetries.GetBlobs(prefix: prefix, cancellationToken: this.blobManager.PartitionErrorHandler.Token);
                this.blobManager.PartitionErrorHandler.Token.ThrowIfCancellationRequested();
                return checkPoints.Select((BlobItem item) =>
                {
                    var guidstring = item.Name.Substring(item.Name.Length - guidLength);
                    var guid = Guid.Parse(guidstring);
                    return guid;
                });
            }
            catch (Azure.RequestFailedException) when (this.terminationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException("Partition was terminated.", this.terminationToken);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.blobManager.PartitionErrorHandler.HandleError(nameof(ReadCheckpointIntentions), "Failed to read checkpoint intentions from storage", e, true, false);
                throw;
            }
        }

        public override Task RunPrefetchSession(IAsyncEnumerable<TrackedObjectKey> keys)
        {
            //TODO
            return Task.CompletedTask;
        }

        public override long? GetCompactionTarget()
        {
            return null;
        }

        public override Task<long> RunCompactionAsync(long target)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
