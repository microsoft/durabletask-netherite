﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using DurableTask.Core.Common;
    using FASTER.core;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.RetryPolicies;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides management of blobs and blob names associated with a partition, and logic for partition lease maintenance and termination.
    /// </summary>
    partial class BlobManager : ICheckpointManager, ILogCommitManager
    {
        readonly uint partitionId;
        readonly CancellationTokenSource shutDownOrTermination;
        readonly CloudStorageAccount cloudStorageAccount;
        readonly CloudStorageAccount secondaryStorageAccount;

        readonly CloudBlobContainer blockBlobContainer;
        readonly CloudBlobContainer pageBlobContainer;
        CloudBlockBlob eventLogCommitBlob;
        CloudBlobDirectory pageBlobPartitionDirectory;
        CloudBlobDirectory blockBlobPartitionDirectory;

        string leaseId;
        readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(45); // max time the lease stays after unclean shutdown
        readonly TimeSpan LeaseRenewal = TimeSpan.FromSeconds(30); // how often we renew the lease
        readonly TimeSpan LeaseSafetyBuffer = TimeSpan.FromSeconds(10); // how much time we want left on the lease before issuing a protected access

        internal CheckpointInfo CheckpointInfo { get; private set; }

        internal FasterTraceHelper TraceHelper { get; private set; }
        internal FasterTraceHelper StorageTracer => this.TraceHelper.IsTracingAtMostDetailedLevel ? this.TraceHelper : null;

        internal bool UseLocalFilesForTestingAndDebugging { get; private set; }

        public IDevice EventLogDevice { get; private set; }
        public IDevice HybridLogDevice { get; private set; }
        public IDevice ObjectLogDevice { get; private set; }

        IDevice[] PsfLogDevices;
        internal CheckpointInfo[] PsfCheckpointInfos { get; }
        int PsfGroupCount => this.PsfCheckpointInfos.Length;
        const int InvalidPsfGroupOrdinal = -1;

        public string ContainerName { get; }

        public CloudBlobContainer BlockBlobContainer => this.blockBlobContainer;
        public CloudBlobContainer PageBlobContainer => this.pageBlobContainer;

        public IPartitionErrorHandler PartitionErrorHandler { get; private set; }

        internal static SemaphoreSlim AsynchronousStorageReadMaxConcurrency = new SemaphoreSlim(Environment.ProcessorCount * 25);
        internal static SemaphoreSlim AsynchronousStorageWriteMaxConcurrency = new SemaphoreSlim(Environment.ProcessorCount * 25);

        volatile System.Diagnostics.Stopwatch leaseTimer;

        public FasterLogSettings EventLogSettings(bool usePremiumStorage) => new FasterLogSettings
        {
            LogDevice = this.EventLogDevice,
            LogCommitManager = this.UseLocalFilesForTestingAndDebugging
                ? new LocalLogCommitManager($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}\\{CommitBlobName}")
                : (ILogCommitManager)this,
            PageSizeBits = 21, // 2MB
            SegmentSizeBits =
                usePremiumStorage ? 35  // 32 GB
                                  : 30, // 1 GB
            MemorySizeBits = 22, // 2MB
        };

        public LogSettings StoreLogSettings(bool usePremiumStorage, uint numPartitions) => new LogSettings
        {
            LogDevice = this.HybridLogDevice,
            ObjectLogDevice = this.ObjectLogDevice,
            PageSizeBits = 17, // 128kB
            MutableFraction = 0.9,
            SegmentSizeBits =
                usePremiumStorage ? 35 // 32 GB
                                  : 32, // 4 GB
            CopyReadsToTail = true,
            MemorySizeBits =
                (numPartitions <= 1) ? 25 : // 32MB
                (numPartitions <= 2) ? 24 : // 16MB
                (numPartitions <= 4) ? 23 : // 8MB
                (numPartitions <= 8) ? 22 : // 4MB
                (numPartitions <= 16) ? 21 : // 2MB
                                        20, // 1MB         
        };

        public void Dispose()
        {
            //TODO figure out what is supposed to go here
        }

        public void PurgeAll()
        {
            //TODO figure out what is supposed to go here
        }

        public CheckpointSettings StoreCheckpointSettings => new CheckpointSettings
        {
            CheckpointManager = this.UseLocalFilesForTestingAndDebugging
                ? new LocalFileCheckpointManager(this.CheckpointInfo, this.LocalCheckpointDirectoryPath, this.GetCheckpointCompletedBlobName())
                : (ICheckpointManager)this,
            CheckPointType = CheckpointType.FoldOver
        };

#if FASTER_SUPPORTS_PSF
        internal PSFRegistrationSettings<TKey> CreatePSFRegistrationSettings<TKey>(uint numberPartitions, int groupOrdinal)
        {
            var storeLogSettings = this.StoreLogSettings(false, numberPartitions);
            return new PSFRegistrationSettings<TKey>
            {
                HashTableSize = FasterKV.HashTableSize,
                LogSettings = new LogSettings()
                {
                    LogDevice = this.PsfLogDevices[groupOrdinal],
                    PageSizeBits = storeLogSettings.PageSizeBits,
                    SegmentSizeBits = storeLogSettings.SegmentSizeBits,
                    MemorySizeBits = storeLogSettings.MemorySizeBits,
                    CopyReadsToTail = false,
                    ReadCacheSettings = storeLogSettings.ReadCacheSettings
                },
                CheckpointSettings = new CheckpointSettings
                {
                    CheckpointManager = this.UseLocalFilesForTestingAndDebugging
                        ? new LocalFileCheckpointManager(this.PsfCheckpointInfos[groupOrdinal], this.LocalPsfCheckpointDirectoryPath(groupOrdinal), this.GetCheckpointCompletedBlobName())
                        : (ICheckpointManager)new PsfBlobCheckpointManager(this, groupOrdinal),
                    CheckPointType = CheckpointType.FoldOver
                }
            };
        }
#endif

        public const int MaxRetries = 10;

        public static BlobRequestOptions BlobRequestOptionsAggressiveTimeout = new BlobRequestOptions()
        {
            RetryPolicy = default, // no automatic retry
            NetworkTimeout = TimeSpan.FromSeconds(2),
            ServerTimeout = TimeSpan.FromSeconds(2),
            MaximumExecutionTime = TimeSpan.FromSeconds(2),
        };

        public static BlobRequestOptions BlobRequestOptionsDefault => new BlobRequestOptions()
        {
            RetryPolicy = default, // no automatic retry
            NetworkTimeout = TimeSpan.FromSeconds(15),
            ServerTimeout = TimeSpan.FromSeconds(15),
            MaximumExecutionTime = TimeSpan.FromSeconds(15),
        };

        public static BlobRequestOptions BlobRequestOptionsWithRetry => new BlobRequestOptions()
        {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(2), MaxRetries),
            NetworkTimeout = TimeSpan.FromSeconds(15),
            ServerTimeout = TimeSpan.FromSeconds(15),
            MaximumExecutionTime = TimeSpan.FromSeconds(15),
        };

        public static TimeSpan GetDelayBetweenRetries(int numAttempts)
            => TimeSpan.FromSeconds(Math.Pow(2, (numAttempts - 1)));

        // For tests only; TODO consider adding PSFs
        internal BlobManager(CloudStorageAccount storageAccount, CloudStorageAccount secondaryStorageAccount, string taskHubName, ILogger logger, Microsoft.Extensions.Logging.LogLevel logLevelLimit, uint partitionId, IPartitionErrorHandler errorHandler)
            : this(storageAccount, secondaryStorageAccount, taskHubName, logger, logLevelLimit, partitionId, errorHandler, 0)
        {
        }

        /// <summary>
        /// Create a blob manager.
        /// </summary>
        /// <param name="storageAccount">The cloud storage account, or null if using local file paths</param>
        /// <param name="secondaryStorageAccount">Optionally, a secondary cloud storage accounts</param>
        /// <param name="taskHubName">The name of the taskhub</param>
        /// <param name="logger">A logger for logging</param>
        /// <param name="logLevelLimit">A limit on log event level emitted</param>
        /// <param name="partitionId">The partition id</param>
        /// <param name="errorHandler">A handler for errors encountered in this partition</param>
        /// <param name="psfGroupCount">Number of PSF groups to be created in FASTER</param>
        public BlobManager(
            CloudStorageAccount storageAccount,
            CloudStorageAccount secondaryStorageAccount,
            string taskHubName,
            ILogger logger,
            Microsoft.Extensions.Logging.LogLevel logLevelLimit,
            uint partitionId, IPartitionErrorHandler errorHandler,
            int psfGroupCount)
        {
            this.cloudStorageAccount = storageAccount;
            this.secondaryStorageAccount = secondaryStorageAccount;
            this.UseLocalFilesForTestingAndDebugging = (storageAccount == null);
            this.ContainerName = GetContainerName(taskHubName);
            this.partitionId = partitionId;
            this.CheckpointInfo = new CheckpointInfo();
            this.PsfCheckpointInfos = Enumerable.Range(0, psfGroupCount).Select(ii => new CheckpointInfo()).ToArray();

            if (!this.UseLocalFilesForTestingAndDebugging)
            {
                CloudBlobClient serviceClient = this.cloudStorageAccount.CreateCloudBlobClient();
                this.blockBlobContainer = serviceClient.GetContainerReference(this.ContainerName);
                serviceClient = this.secondaryStorageAccount.CreateCloudBlobClient();
                this.pageBlobContainer = serviceClient.GetContainerReference(this.ContainerName);
            }

            this.TraceHelper = new FasterTraceHelper(logger, logLevelLimit, this.partitionId, this.UseLocalFilesForTestingAndDebugging ? "none" : this.cloudStorageAccount.Credentials.AccountName, taskHubName);
            this.PartitionErrorHandler = errorHandler;
            this.shutDownOrTermination = CancellationTokenSource.CreateLinkedTokenSource(errorHandler.Token);
        }

        // For testing and debugging with local files
        internal static string LocalFileDirectoryForTestingAndDebugging { get; set; } = $"{Environment.GetEnvironmentVariable("temp")}\\FasterTestStorage";

        string LocalDirectoryPath => $"{LocalFileDirectoryForTestingAndDebugging}\\{this.ContainerName}";

        string PartitionFolderName => $"p{this.partitionId:D2}";
        string PsfGroupFolderName(int groupOrdinal) => $"psfgroup.{groupOrdinal:D3}";

        string LocalCheckpointDirectoryPath => $"{this.LocalDirectoryPath}\\chkpts{this.partitionId:D2}";
        string LocalPsfCheckpointDirectoryPath(int groupOrdinal) => $"{this.LocalDirectoryPath}\\chkpts{this.partitionId:D2}\\psfgroup.{groupOrdinal:D3}";

        const string EventLogBlobName = "commit-log";
        const string CommitBlobName = "commit-lease";
        const string HybridLogBlobName = "store";
        const string ObjectLogBlobName = "store.obj";

        // PSFs do not have an object log
        const string PsfHybridLogBlobName = "store.psf";

        Task LeaseMaintenanceLoopTask = Task.CompletedTask;
        volatile Task NextLeaseRenewalTask = Task.CompletedTask;

        static string GetContainerName(string taskHubName) => taskHubName.ToLowerInvariant() + "-storage";

        public async Task StartAsync()
        {
            if (this.UseLocalFilesForTestingAndDebugging)
            {
                Directory.CreateDirectory($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}");

                this.EventLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}\\{EventLogBlobName}");
                this.HybridLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}\\{HybridLogBlobName}");
                this.ObjectLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}\\{ObjectLogBlobName}");
                this.PsfLogDevices = (from groupOrdinal in Enumerable.Range(0, this.PsfGroupCount)
                                      let deviceName = $"{this.LocalDirectoryPath}\\{this.PartitionFolderName}\\{this.PsfGroupFolderName(groupOrdinal)}\\{PsfHybridLogBlobName}"
                                      select Devices.CreateLogDevice(deviceName)).ToArray();

                // This does not acquire any blob ownership, but is needed for the lease maintenance loop which calls PartitionErrorHandler.TerminateNormally() when done.
                await this.AcquireOwnership();
            }
            else
            {
                await this.blockBlobContainer.CreateIfNotExistsAsync();
                await this.pageBlobContainer.CreateIfNotExistsAsync();
                this.pageBlobPartitionDirectory = this.pageBlobContainer.GetDirectoryReference(this.PartitionFolderName);
                this.blockBlobPartitionDirectory = this.blockBlobContainer.GetDirectoryReference(this.PartitionFolderName);

                this.eventLogCommitBlob = this.blockBlobPartitionDirectory.GetBlockBlobReference(CommitBlobName);

                AzureStorageDevice createDevice(string name) =>
                    new AzureStorageDevice(name, this.blockBlobPartitionDirectory.GetDirectoryReference(name), this.pageBlobPartitionDirectory.GetDirectoryReference(name), this, true);

                var eventLogDevice = createDevice(EventLogBlobName);
                var hybridLogDevice = createDevice(HybridLogBlobName);
                var objectLogDevice = createDevice(ObjectLogBlobName);

                var psfLogDevices = (from groupOrdinal in Enumerable.Range(0, this.PsfGroupCount)
                                     let psfDirectory = this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(groupOrdinal))
                                     select new AzureStorageDevice(PsfHybridLogBlobName, psfDirectory.GetDirectoryReference(PsfHybridLogBlobName), psfDirectory.GetDirectoryReference(PsfHybridLogBlobName), this, true)).ToArray();

                await this.AcquireOwnership();

                this.TraceHelper.FasterProgress("Starting Faster Devices");
                var startTasks = new List<Task>
                {
                    eventLogDevice.StartAsync(),
                    hybridLogDevice.StartAsync(),
                    objectLogDevice.StartAsync()
                };
                startTasks.AddRange(psfLogDevices.Select(psfLogDevice => psfLogDevice.StartAsync()));
                await Task.WhenAll(startTasks);
                this.TraceHelper.FasterProgress("Started Faster Devices");

                this.EventLogDevice = eventLogDevice;
                this.HybridLogDevice = hybridLogDevice;
                this.ObjectLogDevice = objectLogDevice;
                this.PsfLogDevices = psfLogDevices;
            }
        }

        internal void ClosePSFDevices() => Array.ForEach(this.PsfLogDevices, logDevice => logDevice.Dispose());

        public void HandleStorageError(string where, string message, string blobName, Exception e, bool isFatal, bool isWarning)
        {
            if (blobName == null)
            {
                this.PartitionErrorHandler.HandleError(where, message, e, isFatal, isWarning);
            }
            else
            {
                this.PartitionErrorHandler.HandleError(where, $"{message} blob={blobName}", e, isFatal, isWarning);
            }
        }

        // clean shutdown, wait for everything, then terminate
        public async Task StopAsync()
        {
            this.shutDownOrTermination.Cancel(); // has no effect if already cancelled

            await this.LeaseMaintenanceLoopTask; // wait for loop to terminate cleanly
        }

        public static async Task DeleteTaskhubStorageAsync(CloudStorageAccount account, string taskHubName)
        {
            var containerName = GetContainerName(taskHubName);

            if (account is null)
            {
                DirectoryInfo di = new DirectoryInfo($"{LocalFileDirectoryForTestingAndDebugging}\\{containerName}");
                if (di.Exists)
                {
                    di.Delete(true);
                }
            }
            else
            {
                CloudBlobClient serviceClient = account.CreateCloudBlobClient();
                var blobContainer = serviceClient.GetContainerReference(containerName);

                if (await blobContainer.ExistsAsync())
                {
                    // do a complete deletion of all contents of this directory
                    var tasks = blobContainer.ListBlobs(null, true)
                                             .Where(blob => blob.GetType() == typeof(CloudBlob) || blob.GetType().BaseType == typeof(CloudBlob))
                                             .Select(blob => BlobUtils.ForceDeleteAsync((CloudBlob)blob))
                                             .ToArray();
                    await Task.WhenAll(tasks);
                }

                // We are not deleting the container itself because it creates problems when trying to recreate
                // the same container soon afterwards so we leave an empty container behind. Oh well.
            }
        }

        public ValueTask ConfirmLeaseIsGoodForAWhileAsync()
        {
            if (this.leaseTimer?.Elapsed < this.LeaseDuration - this.LeaseSafetyBuffer)
            {
                return default;
            }
            this.TraceHelper.LeaseProgress("Access is waiting for fresh lease");
            return new ValueTask(this.NextLeaseRenewalTask);
        }

        public void ConfirmLeaseIsGoodForAWhile()
        {
            if (this.leaseTimer?.Elapsed < this.LeaseDuration - this.LeaseSafetyBuffer)
            {
                return;
            }
            this.TraceHelper.LeaseProgress("Access is waiting for fresh lease");
            this.NextLeaseRenewalTask.Wait();
            this.TraceHelper.LeaseProgress("Access has fresh lease now");
        }

        async Task AcquireOwnership()
        {
            var newLeaseTimer = new System.Diagnostics.Stopwatch();
            int numAttempts = 0;

            while (true)
            {
                this.PartitionErrorHandler.Token.ThrowIfCancellationRequested();
                numAttempts++;

                try
                {
                    newLeaseTimer.Restart();

                    if (!this.UseLocalFilesForTestingAndDebugging)
                    {
                        this.leaseId = await this.eventLogCommitBlob.AcquireLeaseAsync(
                            this.LeaseDuration,
                            null,
                            accessCondition: null,
                            options: BlobManager.BlobRequestOptionsDefault,
                            operationContext: null,
                            cancellationToken: this.PartitionErrorHandler.Token).ConfigureAwait(false);
                        this.TraceHelper.LeaseAcquired();
                    }

                    this.leaseTimer = newLeaseTimer;
                    this.LeaseMaintenanceLoopTask = Task.Run(() => this.MaintenanceLoopAsync());
                    return;
                }
                catch (StorageException ex) when (BlobUtils.LeaseConflictOrExpired(ex))
                {
                    this.TraceHelper.LeaseProgress("Waiting for lease");

                    // the previous owner has not released the lease yet, 
                    // try again until it becomes available, should be relatively soon
                    // as the transport layer is supposed to shut down the previous owner when starting this
                    await Task.Delay(TimeSpan.FromSeconds(1), this.PartitionErrorHandler.Token).ConfigureAwait(false);

                    continue;
                }
                catch (StorageException ex) when (BlobUtils.BlobDoesNotExist(ex))
                {
                    try
                    {
                        // Create blob with empty content, then try again
                        await this.PerformWithRetriesAsync(
                            null,
                            false,
                            "CloudBlockBlob.UploadFromByteArrayAsync",
                            "(Create commit blob)",
                            this.eventLogCommitBlob.Name,
                            2000,
                            true,
                            async (numAttempts) =>
                            {
                                try
                                {
                                    await this.eventLogCommitBlob.UploadFromByteArrayAsync(Array.Empty<byte>(), 0, 0);
                                }
                                catch (StorageException ex2) when (BlobUtils.LeaseConflictOrExpired(ex2))
                                {
                                    // creation race, try from top
                                    this.TraceHelper.LeaseProgress("Creation race observed, retrying");
                                }
                            });

                        continue;
                    }
                    catch (StorageException ex2) when (BlobUtils.LeaseConflictOrExpired(ex2))
                    {
                        // creation race, try from top
                        this.TraceHelper.LeaseProgress("Creation race observed, retrying");
                        continue;
                    }
                }
                catch (StorageException ex) when (numAttempts < BlobManager.MaxRetries && BlobUtils.IsTransientStorageError(ex, this.PartitionErrorHandler.Token))
                {
                    if (BlobUtils.IsTimeout(ex))
                    {
                        this.TraceHelper.FasterPerfWarning($"Lease acquisition timed out, retrying now");
                    }
                    else
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.TraceHelper.FasterPerfWarning($"Lease acquisition experienced transient error, retrying in {nextRetryIn}");
                        await Task.Delay(nextRetryIn);
                    }
                    continue;
                }
                catch (Exception e)
                {
                    this.PartitionErrorHandler.HandleError(nameof(AcquireOwnership), "Could not acquire partition lease", e, true, false);
                    throw;
                }
            }
        }

        public async Task RenewLeaseTask()
        {
            try
            {
                this.shutDownOrTermination.Token.ThrowIfCancellationRequested();

                AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };
                var nextLeaseTimer = new System.Diagnostics.Stopwatch();
                nextLeaseTimer.Start();

                if (!this.UseLocalFilesForTestingAndDebugging)
                {
                    this.TraceHelper.LeaseProgress($"Renewing lease at {this.leaseTimer.Elapsed.TotalSeconds - this.LeaseDuration.TotalSeconds}s");
                    await this.eventLogCommitBlob.RenewLeaseAsync(acc, this.PartitionErrorHandler.Token)
                        .ConfigureAwait(false);
                    this.TraceHelper.LeaseProgress($"Renewed lease at {this.leaseTimer.Elapsed.TotalSeconds - this.LeaseDuration.TotalSeconds}s");
                }

                this.leaseTimer = nextLeaseTimer;
            }
            catch (OperationCanceledException)
            {
                // o.k. during termination or shutdown
            }
            catch (Exception)
            {
                this.TraceHelper.LeaseProgress("Failed to renew lease");
                throw;
            }
        }

        public async Task MaintenanceLoopAsync()
        {
            this.TraceHelper.LeaseProgress("Started lease maintenance loop");
            try
            {
                while (true)
                {
                    int timeLeft = (int)(this.LeaseRenewal - this.leaseTimer.Elapsed).TotalMilliseconds;

                    if (timeLeft <= 0)
                    {
                        this.NextLeaseRenewalTask = this.RenewLeaseTask();
                    }
                    else
                    {
                        this.NextLeaseRenewalTask = LeaseTimer.Instance.Schedule(timeLeft, this.RenewLeaseTask, this.shutDownOrTermination.Token);
                    }

                    // wait for successful renewal, or exit the loop as this throws
                    await this.NextLeaseRenewalTask.ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // it's o.k. to cancel while waiting
                this.TraceHelper.LeaseProgress("Lease renewal loop cleanly canceled");
            }
            catch (StorageException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
            {
                // it's o.k. to cancel a lease renewal
                this.TraceHelper.LeaseProgress("Lease renewal storage operation canceled");
            }
            catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
            {
                // We lost the lease to someone else. Terminate ownership immediately.
                this.PartitionErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Lost partition lease", ex, true, true);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.PartitionErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Could not maintain partition lease", e, true, false);
            }

            this.TraceHelper.LeaseProgress("Exited lease maintenance loop");

            if (this.PartitionErrorHandler.IsTerminated)
            {
                // this is an unclean shutdown, so we let the lease expire to protect straggling storage accesses
                this.TraceHelper.LeaseProgress("Leaving lease to expire on its own");
            }
            else
            {
                if (!this.UseLocalFilesForTestingAndDebugging)
                {
                    try
                    {
                        this.TraceHelper.LeaseProgress("Releasing lease");

                        AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };

                        await this.eventLogCommitBlob.ReleaseLeaseAsync(
                            accessCondition: acc,
                            options: BlobManager.BlobRequestOptionsDefault,
                            operationContext: null,
                            cancellationToken: this.PartitionErrorHandler.Token).ConfigureAwait(false);

                        this.TraceHelper.LeaseReleased();
                    }
                    catch (OperationCanceledException)
                    {
                        // it's o.k. if termination is triggered while waiting
                    }
                    catch (StorageException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
                    {
                        // it's o.k. if termination is triggered while we are releasing the lease
                    }
                    catch (Exception e)
                    {
                        // we swallow, but still report exceptions when releasing a lease
                        this.PartitionErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Could not release partition lease during shutdown", e, false, true);
                    }
                }

                this.PartitionErrorHandler.TerminateNormally();
            }

            this.TraceHelper.LeaseProgress("Blob manager stopped");
        }

        #region Blob Name Management

        string GetCheckpointCompletedBlobName() => $"last-checkpoint.json";

        string GetIndexCheckpointMetaBlobName(Guid token) => $"index-checkpoints/{token}/info.dat";

        (string, string) GetPrimaryHashTableBlobName(Guid token) => ($"index-checkpoints/{token}", "ht.dat");

        string GetHybridLogCheckpointMetaBlobName(Guid token) => $"cpr-checkpoints/{token}/info.dat";

        (string, string) GetLogSnapshotBlobName(Guid token) => ($"cpr-checkpoints/{token}", "snapshot.dat");

        (string, string) GetObjectLogSnapshotBlobName(Guid token) => ($"cpr-checkpoints/{token}", "snapshot.obj.dat");

        #endregion

        #region ILogCommitManager

        void ILogCommitManager.Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            this.StorageTracer?.FasterStorageProgress($"ILogCommitManager.Commit Called beginAddress={beginAddress} untilAddress={untilAddress}");

            AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };

            this.PerformWithRetries(
                false,
                "CloudBlockBlob.UploadFromByteArray",
                "(commit log)",
                this.eventLogCommitBlob.Name,
                1000,
                true,
                (int numAttempts) =>
                {
                    try
                    {
                        var blobRequestOptions = numAttempts > 2 ? BlobManager.BlobRequestOptionsDefault : BlobManager.BlobRequestOptionsAggressiveTimeout;
                        this.eventLogCommitBlob.UploadFromByteArray(commitMetadata, 0, commitMetadata.Length, acc, blobRequestOptions);
                        return true;
                    }
                    catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
                    {
                        // We lost the lease to someone else. Terminate ownership immediately.
                        this.TraceHelper.LeaseLost(nameof(ILogCommitManager.Commit));
                        this.HandleStorageError(nameof(ILogCommitManager.Commit), "could not commit because of lost lease", this.eventLogCommitBlob?.Name, ex, true, this.PartitionErrorHandler.IsTerminated);
                        throw;
                    }
                    catch (StorageException ex) when (BlobUtils.LeaseExpired(ex) && numAttempts < BlobManager.MaxRetries)
                    {
                        // if we get here, the lease renewal task did not complete in time
                        // give it another chance to complete
                        this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: wait for next renewal");
                        this.NextLeaseRenewalTask.Wait();
                        this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: renewal complete");
                        return false;
                    }
                });
        }
    

        IEnumerable<long> ILogCommitManager.ListCommits()
        {
            // we only use a single commit file in this implementation
            yield return 0;
        }

        byte[] ILogCommitManager.GetCommitMetadata(long commitNum)
        {
            this.StorageTracer?.FasterStorageProgress($"ILogCommitManager.GetCommitMetadata Called (thread={Thread.CurrentThread.ManagedThreadId})");
            AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };
            using var stream = new MemoryStream();

            this.PerformWithRetries(
               false,
               "CloudBlockBlob.DownloadToStream",
               "(read commit log)",
               this.eventLogCommitBlob.Name,
               1000,
               true,
               (int numAttempts) =>
               {
                   if (numAttempts > 0)
                   {
                       stream.Seek(0, SeekOrigin.Begin);
                   }

                   try
                   {
                       var blobRequestOptions = numAttempts > 2 ? BlobManager.BlobRequestOptionsDefault : BlobManager.BlobRequestOptionsAggressiveTimeout;
                       this.eventLogCommitBlob.DownloadToStream(stream, acc, blobRequestOptions);
                       return true;
                   }
                   catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
                   {
                       // We lost the lease to someone else. Terminate ownership immediately.
                       this.TraceHelper.LeaseLost(nameof(ILogCommitManager.GetCommitMetadata));
                       this.HandleStorageError(nameof(ILogCommitManager.Commit), "could not read latest commit due to lost lease", this.eventLogCommitBlob?.Name, ex, true, this.PartitionErrorHandler.IsTerminated);
                       throw;
                   }
                   catch (StorageException ex) when (BlobUtils.LeaseExpired(ex) && numAttempts < BlobManager.MaxRetries)
                   {
                       // if we get here, the lease renewal task did not complete in time
                       // give it another chance to complete
                       this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: wait for next renewal");
                       this.NextLeaseRenewalTask.Wait();
                       this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: renewal complete");
                       return false;
                   }
               });

            var bytes = stream.ToArray();
            this.StorageTracer?.FasterStorageProgress($"ILogCommitManager.GetCommitMetadata Returned {bytes?.Length ?? null} bytes");
            return bytes.Length == 0 ? null : bytes;
        }

#endregion

        #region ICheckpointManager

        void ICheckpointManager.InitializeIndexCheckpoint(Guid indexToken)
        {
            // there is no need to create empty directories in a blob container
        }

        void ICheckpointManager.InitializeLogCheckpoint(Guid logToken)
        {
            // there is no need to create empty directories in a blob container
        }

        #region Call-throughs to actual implementation; separated for PSFs

        void ICheckpointManager.CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
            => this.CommitIndexCheckpoint(indexToken, commitMetadata, InvalidPsfGroupOrdinal);

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
            => this.CommitLogCheckpoint(logToken, commitMetadata, InvalidPsfGroupOrdinal);

        byte[] ICheckpointManager.GetIndexCheckpointMetadata(Guid indexToken)
            => this.GetIndexCheckpointMetadata(indexToken, InvalidPsfGroupOrdinal);

        byte[] ICheckpointManager.GetLogCheckpointMetadata(Guid logToken)
            => this.GetLogCheckpointMetadata(logToken, InvalidPsfGroupOrdinal);

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
            => this.GetIndexDevice(indexToken, InvalidPsfGroupOrdinal);

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
            => this.GetSnapshotLogDevice(token, InvalidPsfGroupOrdinal);

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
            => this.GetSnapshotObjectLogDevice(token, InvalidPsfGroupOrdinal);

        IEnumerable<Guid> ICheckpointManager.GetIndexCheckpointTokens()
        {
            var indexToken = this.CheckpointInfo.IndexToken;
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetLogCheckpointTokens returned logToken={indexToken}");
            yield return indexToken;
        }

        IEnumerable<Guid> ICheckpointManager.GetLogCheckpointTokens()
        {
            var logToken = this.CheckpointInfo.LogToken;
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetLogCheckpointTokens returned logToken={logToken}");
            yield return logToken;
        }

        internal Task FindCheckpointsAsync()
        {
            var tasks = new List<Task>();
            tasks.Add(FindCheckpoint(InvalidPsfGroupOrdinal));
            for (int i = 0; i < this.PsfGroupCount; i++)
            {
                tasks.Add(FindCheckpoint(i));
            }
            return Task.WhenAll(tasks);

            async Task FindCheckpoint(int psfGroupOrdinal)
            {
                var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
                CloudBlockBlob checkpointCompletedBlob = null;
                try
                {
                    var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                    checkpointCompletedBlob = partDir.GetBlockBlobReference(this.GetCheckpointCompletedBlobName());
                    await this.ConfirmLeaseIsGoodForAWhileAsync();
                    var jsonString = await checkpointCompletedBlob.DownloadTextAsync();
                    var checkpointInfo = JsonConvert.DeserializeObject<CheckpointInfo>(jsonString);
                    if (isPsf)
                        this.PsfCheckpointInfos[psfGroupOrdinal] = checkpointInfo;
                    else
                        this.CheckpointInfo = checkpointInfo;
                }
                catch (Exception e)
                {
                    this.HandleStorageError(nameof(FindCheckpoint), "could not determine latest checkpoint", checkpointCompletedBlob?.Name, e, true, this.PartitionErrorHandler.IsTerminated);
                    throw;
                }
            }
        }

        #endregion

        #region Actual implementation; separated for PSFs

        (bool, string) IsPsfOrPrimary(int psfGroupOrdinal)
        {
            var isPsf = psfGroupOrdinal != InvalidPsfGroupOrdinal;
            return (isPsf, isPsf ? $"PSF Group {psfGroupOrdinal}" : "Primary FKV");
        }

        internal void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata, int psfGroupOrdinal)
        {
            var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitIndexCheckpoint Called on {tag}, indexToken={indexToken}");
            var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
            var metaFileBlob = partDir.GetBlockBlobReference(this.GetIndexCheckpointMetaBlobName(indexToken));

            this.PerformWithRetries(
             false,
             "CloudBlockBlob.OpenWrite",
             "(CommitIndexCheckpoint)",
             metaFileBlob.Name,
             1000,
             true,
             (numAttempts) =>
             {
                 using (var blobStream = metaFileBlob.OpenWrite())
                 {
                     using var writer = new BinaryWriter(blobStream);
                     writer.Write(commitMetadata.Length);
                     writer.Write(commitMetadata);
                     writer.Flush();
                     return true;
                 }
             });

            (isPsf ? this.PsfCheckpointInfos[psfGroupOrdinal] : this.CheckpointInfo).IndexToken = indexToken;
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitIndexCheckpoint Returned from {tag}, target={metaFileBlob.Name}");
        }

        internal void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata, int psfGroupOrdinal)
        {
            var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitLogCheckpoint Called on {tag}, logToken={logToken}");
            var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
            var metaFileBlob = partDir.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlobName(logToken));

            this.PerformWithRetries(
                false,
                "CloudBlockBlob.OpenWrite",
                "(CommitLogCheckpoint)",
                metaFileBlob.Name,
                1000,
                true,
                (numAttempts) =>
                {
                    using (var blobStream = metaFileBlob.OpenWrite())
                    {
                        using var writer = new BinaryWriter(blobStream);
                        writer.Write(commitMetadata.Length);
                        writer.Write(commitMetadata);
                        writer.Flush();
                        return true;
                    }
                });

            (isPsf ? this.PsfCheckpointInfos[psfGroupOrdinal] : this.CheckpointInfo).LogToken = logToken;
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitLogCheckpoint Returned from {tag}, target={metaFileBlob.Name}");
        }

        internal byte[] GetIndexCheckpointMetadata(Guid indexToken, int psfGroupOrdinal)
        {
            var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Called on {tag}, indexToken={indexToken}");
            var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
            var metaFileBlob = partDir.GetBlockBlobReference(this.GetIndexCheckpointMetaBlobName(indexToken));
            byte[] result = null;

            this.PerformWithRetries(
               false,
               "CloudBlockBlob.OpenRead",
               "(GetIndexCheckpointMetadata)",
               metaFileBlob.Name,
               1000,
               true,
               (numAttempts) =>
               {
                   using var blobstream = metaFileBlob.OpenRead();
                   using var reader = new BinaryReader(blobstream);
                   var len = reader.ReadInt32();
                   result = reader.ReadBytes(len);
                   return true;
               });

            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Returned {result?.Length ?? null} bytes from {tag}, target={metaFileBlob.Name}");
            return result;
        }
        internal byte[] GetLogCheckpointMetadata(Guid logToken, int psfGroupOrdinal)
        {
            var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Called on {tag}, logToken={logToken}");
            var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
            var metaFileBlob = partDir.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlobName(logToken));
            byte[] result = null;

            this.PerformWithRetries(
                false,
                "CloudBlockBlob.OpenRead",
                "(GetLogCheckpointMetadata)",
                metaFileBlob.Name,
                1000,
                true,
                (numAttempts) => 
                {
                    using var blobstream = metaFileBlob.OpenRead();
                    using var reader = new BinaryReader(blobstream);
                    var len = reader.ReadInt32();
                    result = reader.ReadBytes(len);
                    return true;
                });
          
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Returned {result?.Length ?? null} bytes from {tag}, target={metaFileBlob.Name}");
            return result;
        }

        internal IDevice GetIndexDevice(Guid indexToken, int psfGroupOrdinal)
        {
            var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexDevice Called on {tag}, indexToken={indexToken}");
            var (path, blobName) = this.GetPrimaryHashTableBlobName(indexToken);
            var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
            var blobDirectory = partDir.GetDirectoryReference(path);
            var device = new AzureStorageDevice(blobName, blobDirectory, blobDirectory, this, false); // we don't need a lease since the token provides isolation
            device.StartAsync().Wait();
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexDevice Returned from {tag}, target={blobDirectory.Prefix}{blobName}");
            return device;
        }

        internal IDevice GetSnapshotLogDevice(Guid token, int psfGroupOrdinal)
        {
            var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotLogDevice Called on {tag}, token={token}");
            var (path, blobName) = this.GetLogSnapshotBlobName(token);
            var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
            var blobDirectory = partDir.GetDirectoryReference(path);
            var device = new AzureStorageDevice(blobName, blobDirectory, blobDirectory, this, false); // we don't need a lease since the token provides isolation
            device.StartAsync().Wait();
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotLogDevice Returned from {tag}, blobDirectory={blobDirectory} blobName={blobName}");
            return device;
        }

        internal IDevice GetSnapshotObjectLogDevice(Guid token, int psfGroupOrdinal)
        {
            var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotObjectLogDevice Called on {tag}, token={token}");
            var (path, blobName) = this.GetObjectLogSnapshotBlobName(token);
            var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
            var blobDirectory = partDir.GetDirectoryReference(path);
            var device = new AzureStorageDevice(blobName, blobDirectory, blobDirectory, this, false); // we don't need a lease since the token provides isolation
            device.StartAsync().Wait();
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotObjectLogDevice Returned from {tag}, blobDirectory={blobDirectory} blobName={blobName}");
            return device;
        }

        #endregion

        internal async Task FinalizeCheckpointCompletedAsync()
        {
            // write the final file that has all the checkpoint info
            void writeLocal(string path, string text)
                => File.WriteAllText(Path.Combine(path, this.GetCheckpointCompletedBlobName()), text);

            async Task writeBlob(CloudBlobDirectory partDir, string text)
            {
                var checkpointCompletedBlob = partDir.GetBlockBlobReference(this.GetCheckpointCompletedBlobName());
                await this.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false); // the lease protects the checkpoint completed file
                await this.PerformWithRetriesAsync(
                    BlobManager.AsynchronousStorageWriteMaxConcurrency,
                    true,
                    "CloudBlockBlob.UploadTextAsync",
                    "(finalize checkpoint)",
                    checkpointCompletedBlob.Name,
                    1000,
                    true,
                    (numAttempts) => checkpointCompletedBlob.UploadTextAsync(text));
            }

            // Primary FKV
            {
                var jsonText = JsonConvert.SerializeObject(this.CheckpointInfo, Formatting.Indented);
                if (this.UseLocalFilesForTestingAndDebugging)
                    writeLocal(this.LocalCheckpointDirectoryPath, jsonText);
                else
                    await writeBlob(this.blockBlobPartitionDirectory, jsonText);
            }

            // PSFs
            for (var ii = 0; ii < this.PsfLogDevices.Length; ++ii)
            {
                var jsonText = JsonConvert.SerializeObject(this.PsfCheckpointInfos[ii], Formatting.Indented);
                if (this.UseLocalFilesForTestingAndDebugging)
                    writeLocal(this.LocalPsfCheckpointDirectoryPath(ii), jsonText);
                else
                    await writeBlob(this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(ii)), jsonText);
            }
        }
 
        #endregion
    }
}
