//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation. All rights reserved.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Storage;
    using Microsoft.Extensions.Logging;

    class FasterStorage : IPartitionState
    {
        // if used as a "azure storage connection string", causes Faster to use local file storage instead
        public const string LocalFileStorageConnectionString = "UseLocalFileStorage";
        readonly CloudStorageAccount storageAccount;
        readonly CloudStorageAccount pageBlobStorageAccount;
        readonly string taskHubName;
        readonly ILogger logger;

        Partition partition;
        BlobManager blobManager;
        LogWorker logWorker;
        StoreWorker storeWorker;
        FasterLog log;
        TrackedObjectStore store;

        CancellationToken terminationToken;

        internal FasterTraceHelper TraceHelper { get; private set; }

        public FasterStorage(string connectionString, string premiumStorageConnectionString, string taskHubName, ILoggerFactory loggerFactory)
        {
            if (connectionString != LocalFileStorageConnectionString)
            {
                this.storageAccount = CloudStorageAccount.Parse(connectionString);
            }
            if (!string.IsNullOrEmpty(premiumStorageConnectionString))
            {
                this.pageBlobStorageAccount = CloudStorageAccount.Parse(premiumStorageConnectionString);
            }
            else
            {
                this.pageBlobStorageAccount = this.storageAccount;
            }
            this.taskHubName = taskHubName;
            this.logger = loggerFactory.CreateLogger($"{NetheriteOrchestrationService.LoggerCategoryName}.FasterStorage");
        }

        public static Task DeleteTaskhubStorageAsync(string connectionString, string taskHubName)
        {
            var storageAccount = (connectionString != LocalFileStorageConnectionString) ? CloudStorageAccount.Parse(connectionString) : null;
            return BlobManager.DeleteTaskhubStorageAsync(storageAccount, taskHubName);
        }

        public async Task<long> CreateOrRestoreAsync(Partition partition, IPartitionErrorHandler errorHandler, long firstInputQueuePosition)
        {
            this.partition = partition;
            this.terminationToken = errorHandler.Token;

#if FASTER_SUPPORTS_PSF
            int psfCount = partition.Settings.UsePSFQueries ? FasterKV.PSFCount : 0;
#else
            int psfCount = 0;
#endif

            this.blobManager = new BlobManager(
                this.storageAccount, 
                this.pageBlobStorageAccount, 
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
                this.store = new FasterKV(this.partition, this.blobManager);
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
                    this.store.Recover(out long commitLogPosition, out long inputQueuePosition);
                    this.storeWorker.SetCheckpointPositionsAfterRecovery(commitLogPosition, inputQueuePosition);

                    // truncate the log in case the truncation did not commit after the checkpoint was taken
                    this.logWorker.SetLastCheckpointPosition(commitLogPosition);

                    this.TraceHelper.FasterCheckpointLoaded(this.storeWorker.CommitLogPosition, this.storeWorker.InputQueuePosition, this.store.StoreStats.Get(), stopwatch.ElapsedMilliseconds);
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError("loading checkpoint", e);
                    throw;
                }

                this.partition.Assert(!FASTER.core.LightEpoch.AnyInstanceProtected());

                try
                {
                    if (this.log.TailAddress > (long)this.storeWorker.CommitLogPosition)
                    {
                        // replay log as the store checkpoint lags behind the log
                        await this.storeWorker.ReplayCommitLog(this.logWorker).ConfigureAwait(false);
                    }
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

            var ignoredTask = this.IdleLoop();

            return this.storeWorker.InputQueuePosition;
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

        public void SubmitExternalEvents(IList<PartitionEvent> evts)
        {
            this.logWorker.SubmitExternalEvents(evts);
        }

        public void SubmitInternalEvent(PartitionEvent evt)
        {
            this.logWorker.SubmitInternalEvent(evt);
        }

        async Task IdleLoop()
        {
            while (true)
            {
                await Task.Delay(StoreWorker.IdlingPeriod, this.terminationToken).ConfigureAwait(false);

                //await this.TestStorageLatency();

                if (this.terminationToken.IsCancellationRequested)
                {
                    break;
                }

                // periodically bump the store worker so it can check if enough time has elapsed for doing a checkpoint or a load publish
                this.storeWorker.Notify();
            }
        }
    }
}