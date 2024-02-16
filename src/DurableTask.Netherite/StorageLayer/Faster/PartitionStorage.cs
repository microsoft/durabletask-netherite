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

    class PartitionStorage : IPartitionState
    {
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly string taskHubName;
        readonly string pathPrefix;
        readonly ILogger logger;
        readonly ILogger performanceLogger;
        readonly MemoryTracker memoryTracker;
        //readonly CloudStorageAccount storageAccount;
        //readonly string localFileDirectory;
        //readonly CloudStorageAccount pageBlobStorageAccount;

        Partition partition;
        BlobManager blobManager;
        LogWorker logWorker;
        StoreWorker storeWorker;
        FasterLog log;
        TrackedObjectStore store;
        Timer hangCheckTimer;

        CancellationToken terminationToken;
        Task terminationTokenTask;

        internal FasterTraceHelper TraceHelper { get; private set; }

        public long TargetMemorySize { get; set; }

        public PartitionStorage(NetheriteOrchestrationServiceSettings settings, string pathPrefix, MemoryTracker memoryTracker, ILogger logger, ILogger performanceLogger)
        {
            this.settings = settings;
            this.taskHubName = settings.HubName;
            this.pathPrefix = pathPrefix;
            this.logger = logger;
            this.performanceLogger = performanceLogger;
            this.memoryTracker = memoryTracker;

        

            if (settings.TestHooks?.CacheDebugger != null)
            {
                settings.TestHooks.CacheDebugger.MemoryTracker = this.memoryTracker;
            }
        }

        async Task<T> TerminationWrapper<T>(Task<T> what)
        {
            // wrap a task so the await is canceled if this partition terminates
            await Task.WhenAny(what, this.terminationTokenTask);
            this.terminationToken.ThrowIfCancellationRequested();
            return await what;
        }

        async Task TerminationWrapper(Task what)
        {
            // wrap a task so the await is canceled if this partition terminates
            await Task.WhenAny(what, this.terminationTokenTask);
            this.terminationToken.ThrowIfCancellationRequested();
            await what;
        }

        public async Task<(long,int)> CreateOrRestoreAsync(Partition partition, IPartitionErrorHandler errorHandler, string inputQueueFingerprint)
        {
            this.partition = partition;
            this.terminationToken = errorHandler.Token;
            this.terminationTokenTask = Task.Delay(-1, errorHandler.Token);

            this.blobManager = new BlobManager(
                this.settings,
                this.taskHubName,
                this.pathPrefix,
                partition.Settings.TestHooks?.FaultInjector,
                this.logger,
                this.performanceLogger,
                this.partition.Settings.StorageLogLevelLimit,
                partition.PartitionId,
                errorHandler);

            this.TraceHelper = this.blobManager.TraceHelper;
            this.blobManager.FaultInjector?.Starting(this.blobManager);

            this.TraceHelper.FasterProgress("Starting BlobManager");
            await this.TerminationWrapper(this.blobManager.StartAsync());


            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            this.TraceHelper.FasterProgress("Creating FasterLog");
            this.log = new FasterLog(this.blobManager, partition.Settings);


            this.TraceHelper.FasterProgress("Creating FasterKV");
            this.store = new FasterKV(this.partition, this.blobManager, this.memoryTracker);

            this.TraceHelper.FasterProgress("Creating StoreWorker");
            this.storeWorker = new StoreWorker(this.store, this.partition, this.TraceHelper, this.blobManager, this.terminationToken);

            this.TraceHelper.FasterProgress("Creating LogWorker");
            this.logWorker = this.storeWorker.LogWorker = new LogWorker(this.blobManager, this.log, this.partition, this.storeWorker, this.TraceHelper, this.terminationToken);

            if (this.partition.Settings.TestHooks?.ReplayChecker == null)
            {
                this.hangCheckTimer = new Timer(this.CheckForStuckWorkers, null, 0, 20000);
                errorHandler.OnShutdown += () => this.hangCheckTimer.Dispose();
            }

            bool hasCheckpoint = false;

            try
            {
                // determine if there is a checkpoint we can load from
                hasCheckpoint = await this.store.FindCheckpointAsync(logIsEmpty: this.log.TailAddress == this.log.BeginAddress);
            }
            catch (OperationCanceledException) when (this.partition.ErrorHandler.IsTerminated)
            {
                throw; // normal if canceled
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterStorageError("looking for checkpoint", e);
                throw;
            }

            if (!hasCheckpoint)
            {
                // we are in a situation where either this is a completely fresh partition, or it has only been partially initialized
                // without going all the way to the first checkpoint.
                //
                // we take an (empty) checkpoint immediately (before committing anything to the log), so we can recover from it next time
                try
                {
                    this.TraceHelper.FasterProgress("Creating store");

                    // this is a fresh partition
                    await this.TerminationWrapper(this.storeWorker.Initialize(this.log.BeginAddress, inputQueueFingerprint));

                    await this.TerminationWrapper(this.storeWorker.TakeFullCheckpointAsync("initial checkpoint").AsTask());
                    this.TraceHelper.FasterStoreCreated(this.storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
                }
                catch (OperationCanceledException) when (this.partition.ErrorHandler.IsTerminated)
                {
                    throw; // normal if canceled
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
                    var recovered = await this.TerminationWrapper(this.store.RecoverAsync());
                    this.storeWorker.SetCheckpointPositionsAfterRecovery(recovered.commitLogPosition, recovered.inputQueuePosition, recovered.inputQueueFingerprint);

                    // truncate the log in case the truncation did not commit after the checkpoint was taken
                    this.logWorker.SetLastCheckpointPosition(recovered.commitLogPosition);

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

                this.TraceHelper.FasterProgress($"Replaying log length={this.log.TailAddress - this.storeWorker.CommitLogPosition} range={this.storeWorker.CommitLogPosition}-{this.log.TailAddress}");

                try
                {
                    if (this.log.TailAddress > (long)this.storeWorker.CommitLogPosition)
                    {
                        // replay log as the store checkpoint lags behind the log
                        await this.TerminationWrapper(this.storeWorker.ReplayCommitLog(this.logWorker));
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
                this.storeWorker.RestartThingsAtEndOfRecovery(inputQueueFingerprint, this.blobManager.IncarnationTimestamp);

                this.TraceHelper.FasterProgress("Recovery complete");
            }
            this.blobManager.FaultInjector?.Started(this.blobManager);
            return this.storeWorker.InputQueuePosition;
        }

        public void StartProcessing()
        {
            this.storeWorker.StartProcessing();
            this.logWorker.StartProcessing();
        }

        internal void CheckForStuckWorkers(object _)
        {
            TimeSpan limit = Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromMinutes(3);

            if (this.blobManager.PartitionErrorHandler.IsTerminated)
            {
                return; // partition is already terminated, no point in checking for hangs
            }

            // check if a store worker is stuck in a specific place. 
            if (this.storeWorker.WatchdogSeesExpiredDeadline())
            {
                this.blobManager.PartitionErrorHandler.HandleError("CheckForStuckWorkers", $"store worker deemed stuck by watchdog", null, true, false);
            }

            // check if any of the workers got stuck in a processing loop
            Check("StoreWorker", this.storeWorker.ProcessingBatchSince);
            Check("LogWorker", this.logWorker.ProcessingBatchSince);
            Check("IntakeWorker", this.logWorker.IntakeWorkerProcessingBatchSince);

            void Check(string workerName, TimeSpan? busySince)
            {
                if (busySince.HasValue && busySince.Value > limit)
                {
                    this.blobManager.PartitionErrorHandler.HandleError("CheckForStuckWorkers", $"batch worker {workerName} has been processing for {busySince.Value}, which exceeds the limit {limit}", null, true, false);
                }
            }
        }

        public async Task CleanShutdown(bool takeFinalCheckpoint)
        {
            this.TraceHelper.FasterProgress("Stopping workers");
            
            // in parallel, finish processing log requests and stop processing store requests
            Task t1 = this.logWorker.PersistAndShutdownAsync();
            Task t2 = this.storeWorker.CancelAndShutdown();

            // observe exceptions if the clean shutdown is not working correctly
            await this.TerminationWrapper(t1);
            await this.TerminationWrapper(t2);

            // if the the settings indicate we want to take a final checkpoint, do so now.
            if (takeFinalCheckpoint)
            {
                this.TraceHelper.FasterProgress("Writing final checkpoint");
                await this.TerminationWrapper(this.storeWorker.TakeFullCheckpointAsync("final checkpoint").AsTask());
            }

            this.TraceHelper.FasterProgress("Stopping BlobManager");
            await this.blobManager.StopAsync();
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