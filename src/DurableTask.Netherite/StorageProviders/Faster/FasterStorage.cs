// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage;
    using Microsoft.Extensions.Logging;

    class FasterStorage : IPartitionState
    {
        readonly CloudStorageAccount storageAccount;
        readonly string localFileDirectory;
        readonly CloudStorageAccount pageBlobStorageAccount;
        readonly string taskHubName;
        readonly ILogger logger;
        readonly MemoryTracker memoryTracker;

        Partition partition;
        BlobManager blobManager;
        LogWorker logWorker;
        StoreWorker storeWorker;
        FasterLog log;
        TrackedObjectStore store;

        CancellationToken terminationToken;

        internal FasterTraceHelper TraceHelper { get; private set; }

        public long TargetMemorySize { get; set; }

        public FasterStorage(NetheriteOrchestrationServiceSettings settings, ILoggerFactory loggerFactory)
        {
            string connectionString = settings.ResolvedStorageConnectionString;
            string pageBlobConnectionString = settings.ResolvedPageBlobStorageConnectionString;

            if (!string.IsNullOrEmpty(settings.UseLocalDirectoryForPartitionStorage))
            {
                this.localFileDirectory = settings.UseLocalDirectoryForPartitionStorage;
            }
            else
            { 
                this.storageAccount = CloudStorageAccount.Parse(connectionString);
            }
            if (pageBlobConnectionString != connectionString && !string.IsNullOrEmpty(pageBlobConnectionString))
            {
                this.pageBlobStorageAccount = CloudStorageAccount.Parse(pageBlobConnectionString);
            }
            else
            {
                this.pageBlobStorageAccount = this.storageAccount;
            }
            this.taskHubName = settings.HubName;
            this.logger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.FasterStorage");
            this.memoryTracker = new MemoryTracker(this);
            this.TargetMemorySize = settings.FasterCacheSizeMB ?? 200 * 1024 * 1024;

            if (settings.CacheDebugger != null)
            {
                settings.CacheDebugger.MemoryTracker = this.memoryTracker;
            }
        }

        public static Task DeleteTaskhubStorageAsync(string connectionString, string pageBlobConnectionString, string localFileDirectory, string taskHubName)
        {
            var storageAccount = string.IsNullOrEmpty(connectionString) ? null : CloudStorageAccount.Parse(connectionString);
            var pageBlobAccount = string.IsNullOrEmpty(pageBlobConnectionString) ? storageAccount : CloudStorageAccount.Parse(pageBlobConnectionString);
            return BlobManager.DeleteTaskhubStorageAsync(storageAccount, pageBlobAccount, localFileDirectory, taskHubName);
        }

        public async Task<long> CreateOrRestoreAsync(Partition partition, IPartitionErrorHandler errorHandler, long firstInputQueuePosition)
        {
            this.partition = partition;
            this.terminationToken = errorHandler.Token;

            int psfCount = 0;

            this.blobManager = new BlobManager(
                this.storageAccount,
                this.pageBlobStorageAccount,
                this.localFileDirectory,
                this.taskHubName,
                this.logger,
                this.partition.Settings.StorageLogLevelLimit,
                partition.PartitionId,
                errorHandler,
                psfCount);

            this.TraceHelper = this.blobManager.TraceHelper;

            this.TraceHelper.FasterProgress("Starting BlobManager");
            await this.blobManager.StartAsync().ConfigureAwait(false);

            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            this.TraceHelper.FasterProgress("Creating FasterLog");
            this.log = new FasterLog(this.blobManager, partition.Settings);

            if (partition.Settings.UseAlternateObjectStore)
            {
                this.TraceHelper.FasterProgress("Creating FasterAlt");
                this.store = new FasterAlt(this.partition, this.blobManager);
            }
            else
            {
                this.TraceHelper.FasterProgress("Creating FasterKV");
                this.store = new FasterKV(this.partition, this.blobManager, this.memoryTracker);
            }

            this.TraceHelper.FasterProgress("Creating StoreWorker");
            this.storeWorker = new StoreWorker(this.store, this.partition, this.TraceHelper, this.blobManager, this.terminationToken);

            this.TraceHelper.FasterProgress("Creating LogWorker");
            this.logWorker = this.storeWorker.LogWorker = new LogWorker(this.blobManager, this.log, this.partition, this.storeWorker, this.TraceHelper, this.terminationToken);

            if (this.log.TailAddress == this.log.BeginAddress)
            {
                // take an (empty) checkpoint immediately to ensure the paths are working
                try
                {
                    this.TraceHelper.FasterProgress("Creating store");

                    // this is a fresh partition
                    await this.storeWorker.Initialize(this.log.BeginAddress, firstInputQueuePosition).ConfigureAwait(false);

                    await this.storeWorker.TakeFullCheckpointAsync("initial checkpoint").ConfigureAwait(false);
                    this.TraceHelper.FasterStoreCreated(this.storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);

                    this.partition.Assert(!FASTER.core.LightEpoch.AnyInstanceProtected());
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError(nameof(CreateOrRestoreAsync), e);
                    throw;
                }
            }
            else
            {
                this.TraceHelper.FasterProgress("Loading checkpoint");

                try
                {
                    // we are recovering the last checkpoint of the store
                    (long commitLogPosition, long inputQueuePosition) = await this.store.RecoverAsync();
                    this.storeWorker.SetCheckpointPositionsAfterRecovery(commitLogPosition, inputQueuePosition);

                    // truncate the log in case the truncation did not commit after the checkpoint was taken
                    this.logWorker.SetLastCheckpointPosition(commitLogPosition);

                    this.TraceHelper.FasterCheckpointLoaded(this.storeWorker.CommitLogPosition, this.storeWorker.InputQueuePosition, this.store.StoreStats.Get(), stopwatch.ElapsedMilliseconds);
                }
                catch(OperationCanceledException) when (this.partition.ErrorHandler.IsTerminated)
                {
                    throw; // normal if recovery was canceled
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError("loading checkpoint", e);
                    throw;
                }

                this.partition.Assert(!FASTER.core.LightEpoch.AnyInstanceProtected());

                this.TraceHelper.FasterProgress($"Replaying log length={this.log.TailAddress - this.storeWorker.CommitLogPosition} range={this.storeWorker.CommitLogPosition}-{this.log.TailAddress}");

                try
                {
                    if (this.log.TailAddress > (long)this.storeWorker.CommitLogPosition)
                    {
                        // replay log as the store checkpoint lags behind the log
                        await this.storeWorker.ReplayCommitLog(this.logWorker).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (this.partition.ErrorHandler.IsTerminated)
                {
                    throw; // normal if recovery was canceled
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError("replaying log", e);
                    throw;
                }

                // restart pending actitivities, timers, work items etc.
                await this.storeWorker.RestartThingsAtEndOfRecovery().ConfigureAwait(false);

                this.TraceHelper.FasterProgress("Recovery complete");
            }
            return this.storeWorker.InputQueuePosition;
        }

        public void StartProcessing()
        {
            this.storeWorker.StartProcessing();
            this.logWorker.StartProcessing();
        }

        public async Task CleanShutdown(bool takeFinalCheckpoint)
        {
            this.TraceHelper.FasterProgress("Stopping workers");
            
            // in parallel, finish processing log requests and stop processing store requests
            Task t1 = this.logWorker.PersistAndShutdownAsync();
            Task t2 = this.storeWorker.CancelAndShutdown();

            // observe exceptions if the clean shutdown is not working correctly
            await t1.ConfigureAwait(false);
            await t2.ConfigureAwait(false);

            // if the the settings indicate we want to take a final checkpoint, do so now.
            if (takeFinalCheckpoint)
            {
                this.TraceHelper.FasterProgress("Writing final checkpoint");
                await this.storeWorker.TakeFullCheckpointAsync("final checkpoint").ConfigureAwait(false);
            }

            this.TraceHelper.FasterProgress("Stopping BlobManager");
            await this.blobManager.StopAsync().ConfigureAwait(false);
        }

        public void SubmitEvents(IList<PartitionEvent> evts)
        {
            this.logWorker.SubmitEvents(evts);
        }

        public void SubmitEvent(PartitionEvent evt)
        {
            this.logWorker.SubmitEvent(evt);
        }

        public void SubmitParallelEvent(PartitionEvent evt)
        {
            this.storeWorker.ProcessInParallel(evt);
        }

        public Task Prefetch(IEnumerable<TrackedObjectKey> keys)
        {
            return this.storeWorker.RunPrefetchSession(keys.ToAsyncEnumerable<TrackedObjectKey>());
        }
    }
}