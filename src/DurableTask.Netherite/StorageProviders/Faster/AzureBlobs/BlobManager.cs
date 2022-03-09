// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using DurableTask.Core.Common;
    using FASTER.core;
    using ImpromptuInterface;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.RetryPolicies;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
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
        readonly CloudStorageAccount pageBlobAccount;
        readonly string taskHubPrefix;

        readonly CloudBlobContainer blockBlobContainer;
        readonly CloudBlobContainer pageBlobContainer;
        CloudBlockBlob eventLogCommitBlob;
        CloudBlobDirectory pageBlobPartitionDirectory;
        CloudBlobDirectory blockBlobPartitionDirectory;

        string leaseId;
        readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(45); // max time the lease stays after unclean shutdown
        readonly TimeSpan LeaseRenewal = TimeSpan.FromSeconds(30); // how often we renew the lease
        readonly TimeSpan LeaseSafetyBuffer = TimeSpan.FromSeconds(10); // how much time we want left on the lease before issuing a protected access

        internal CheckpointInfo CheckpointInfo { get; }

        internal FasterTraceHelper TraceHelper { get; private set; }
        internal FasterTraceHelper StorageTracer => this.TraceHelper.IsTracingAtMostDetailedLevel ? this.TraceHelper : null;

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

        public int PartitionId => (int)this.partitionId;

        public IPartitionErrorHandler PartitionErrorHandler { get; private set; }

        internal static SemaphoreSlim AsynchronousStorageReadMaxConcurrency = new SemaphoreSlim(Math.Min(100, Environment.ProcessorCount * 10));
        internal static SemaphoreSlim AsynchronousStorageWriteMaxConcurrency = new SemaphoreSlim(Math.Min(50, Environment.ProcessorCount * 7));

        volatile System.Diagnostics.Stopwatch leaseTimer;

        internal const long HashTableSize = 1L << 14; // 16 k buckets, 1 MB
        internal const long HashTableSizeBytes = HashTableSize * 64;

        public class FasterTuningParameters
        {
            public int? EventLogPageSizeBits;
            public int? EventLogSegmentSizeBits;
            public int? EventLogMemorySizeBits;
            public int? StoreLogPageSizeBits;
            public int? StoreLogSegmentSizeBits;
            public int? StoreLogMemorySizeBits;
            public double? StoreLogMutableFraction;
            public int? ExpectedObjectSize;
            public int? NumPagesToPreload;
        }

        public FasterLogSettings GetDefaultEventLogSettings(bool useSeparatePageBlobStorage, FasterTuningParameters tuningParameters) => new FasterLogSettings
        {
            LogDevice = this.EventLogDevice,
            LogCommitManager = this.UseLocalFiles
                ? null // TODO: fix this: new LocalLogCommitManager($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}\\{CommitBlobName}")
                : (ILogCommitManager)this,
            PageSizeBits = tuningParameters?.EventLogPageSizeBits ?? 21, // 2MB
            SegmentSizeBits = tuningParameters?.EventLogSegmentSizeBits ??
                (useSeparatePageBlobStorage ? 35  // 32 GB
                                            : 26), // 64 MB
            MemorySizeBits = tuningParameters?.EventLogMemorySizeBits ?? 22, // 2MB
        };

        public LogSettings GetDefaultStoreLogSettings(
            bool useSeparatePageBlobStorage, 
            long upperBoundOnAvailable, 
            FasterTuningParameters tuningParameters)
        {

            var settings = new LogSettings
            {
                LogDevice = this.HybridLogDevice,
                ObjectLogDevice = this.ObjectLogDevice,
                PageSizeBits = tuningParameters?.StoreLogPageSizeBits ?? 10, // 1kB
                MutableFraction = tuningParameters?.StoreLogMutableFraction ?? 0.9,
                SegmentSizeBits = tuningParameters?.StoreLogSegmentSizeBits ??
                    (useSeparatePageBlobStorage ? 35   // 32 GB
                                                : 32), // 4 GB
                CopyReadsToTail = CopyReadsToTail.FromReadOnly,
            };

            // compute a reasonable memory size for the log considering maximally availablee memory, and expansion factor
            if (tuningParameters?.StoreLogMemorySizeBits != null)
            {
                settings.MemorySizeBits = tuningParameters.StoreLogMemorySizeBits.Value;
            }
            else
            {
                double expansionFactor = (24 + ((double)(tuningParameters?.ExpectedObjectSize ?? 216))) / 24;
                long estimate = (long)(upperBoundOnAvailable / expansionFactor);
                int memorybits = 0;
                while (estimate > 0)
                {
                    memorybits++;
                    estimate >>= 1;
                }
                memorybits = Math.Max(settings.PageSizeBits + 2, memorybits); // do not use less than 4 pages
                settings.MemorySizeBits = memorybits;
            }

            return settings;
        }


        static readonly int[] StorageFormatVersion = new int[] {
            1, //initial version
            2, //0.7.0-beta changed singleton storage, and adds dequeue count
            3, //changed organization of files
            4, //use Faster v2, reduced page size
        }; 

        public static string GetStorageFormat(NetheriteOrchestrationServiceSettings settings)
        {
            return JsonConvert.SerializeObject(new StorageFormatSettings()
                {
                    UseAlternateObjectStore = settings.UseAlternateObjectStore,
                    FormatVersion = StorageFormatVersion.Last(),
                },
                serializerSettings);
        }

        [JsonObject]
        class StorageFormatSettings
        {
            // this must stay the same

            [JsonProperty("FormatVersion")]
            public int FormatVersion { get; set; }

            // the following can be changed between versions

            [JsonProperty("UseAlternateObjectStore", DefaultValueHandling = DefaultValueHandling.Ignore)]
            public bool? UseAlternateObjectStore { get; set; }
        }

        static readonly JsonSerializerSettings serializerSettings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.None,
            MissingMemberHandling = MissingMemberHandling.Ignore,
            CheckAdditionalContent = false,
            Formatting = Formatting.None,
        };

        public static void CheckStorageFormat(string format, NetheriteOrchestrationServiceSettings settings)
        {
            try
            {
                var taskhubFormat = JsonConvert.DeserializeObject<StorageFormatSettings>(format, serializerSettings);

                if (taskhubFormat.UseAlternateObjectStore != settings.UseAlternateObjectStore)
                {
                    throw new InvalidOperationException("The Netherite configuration setting 'UseAlternateObjectStore' is incompatible with the existing taskhub.");
                }
                if (taskhubFormat.FormatVersion != StorageFormatVersion.Last())
                {
                    throw new InvalidOperationException($"The current storage format version (={StorageFormatVersion.Last()}) is incompatible with the existing taskhub (={taskhubFormat.FormatVersion}).");
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("The taskhub has an incompatible storage format", e);
            }
        }

        public void Dispose()
        {
            // we do not need to dispose any resources for the commit manager, because any such resources are deleted together with the taskhub
        }

        public void Purge(Guid token)
        {
            throw new NotImplementedException("Purges are handled directly on recovery, not via FASTER");
        }

        public void PurgeAll()
        {
            throw new NotImplementedException("Purges are handled directly on recovery, not via FASTER");
        }

        public void OnRecovery(Guid indexToken, Guid logToken)
        {
            // we handle cleanup of old checkpoints somewhere else
        }

        public CheckpointSettings StoreCheckpointSettings => new CheckpointSettings
        {
            CheckpointManager = this.UseLocalFiles ? (ICheckpointManager)this.LocalCheckpointManager : (ICheckpointManager)this,
        };

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

        /// <summary>
        /// Create a blob manager.
        /// </summary>
        /// <param name="storageAccount">The cloud storage account, or null if using local file paths</param>
        /// <param name="pageBlobAccount">The storage account to use for page blobs</param>
        /// <param name="localFilePath">The local file path, or null if using cloud storage</param>
        /// <param name="taskHubName">The name of the taskhub</param>
        /// <param name="logger">A logger for logging</param>
        /// <param name="logLevelLimit">A limit on log event level emitted</param>
        /// <param name="partitionId">The partition id</param>
        /// <param name="errorHandler">A handler for errors encountered in this partition</param>
        /// <param name="psfGroupCount">Number of PSF groups to be created in FASTER</param>
        public BlobManager(
            CloudStorageAccount storageAccount,
            CloudStorageAccount pageBlobAccount,
            string localFilePath,
            string taskHubName,
            string taskHubPrefix,
            FaultInjector faultInjector,
            ILogger logger,
            Microsoft.Extensions.Logging.LogLevel logLevelLimit,
            uint partitionId, IPartitionErrorHandler errorHandler,
            int psfGroupCount)
        {
            this.cloudStorageAccount = storageAccount;
            this.pageBlobAccount = pageBlobAccount;
            this.UseLocalFiles = (localFilePath != null);
            this.LocalFileDirectoryForTestingAndDebugging = localFilePath;
            this.ContainerName = GetContainerName(taskHubName);
            this.taskHubPrefix = taskHubPrefix;
            this.FaultInjector = faultInjector;
            this.partitionId = partitionId;
            this.CheckpointInfo = new CheckpointInfo();
            this.PsfCheckpointInfos = Enumerable.Range(0, psfGroupCount).Select(ii => new CheckpointInfo()).ToArray();

            if (!this.UseLocalFiles)
            {
                CloudBlobClient serviceClient = this.cloudStorageAccount.CreateCloudBlobClient();
                this.blockBlobContainer = serviceClient.GetContainerReference(this.ContainerName);

                if (pageBlobAccount == storageAccount)
                {
                    this.pageBlobContainer = this.BlockBlobContainer;
                }
                else
                {
                    serviceClient = this.pageBlobAccount.CreateCloudBlobClient();
                    this.pageBlobContainer = serviceClient.GetContainerReference(this.ContainerName);
                }
            }
            else
            {
                this.LocalCheckpointManager = new LocalFileCheckpointManager(
                    this.CheckpointInfo,
                    this.LocalCheckpointDirectoryPath,
                    this.GetCheckpointCompletedBlobName());
            }

            this.TraceHelper = new FasterTraceHelper(logger, logLevelLimit, this.partitionId, this.UseLocalFiles ? "none" : this.cloudStorageAccount.Credentials.AccountName, taskHubName);
            this.PartitionErrorHandler = errorHandler;
            this.shutDownOrTermination = CancellationTokenSource.CreateLinkedTokenSource(errorHandler.Token);
        }

        string PartitionFolderName => $"{this.taskHubPrefix}p{this.partitionId:D2}";
        string PsfGroupFolderName(int groupOrdinal) => $"psfgroup.{groupOrdinal:D3}";

        // For testing and debugging with local files
        bool UseLocalFiles { get; }
        LocalFileCheckpointManager LocalCheckpointManager { get; }
        string LocalFileDirectoryForTestingAndDebugging { get; }
        string LocalDirectoryPath => $"{this.LocalFileDirectoryForTestingAndDebugging}\\{this.ContainerName}";
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

        public static string GetContainerName(string taskHubName) => taskHubName.ToLowerInvariant() + "-storage";

        public async Task StartAsync()
        {
            if (this.UseLocalFiles)
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
                this.blockBlobPartitionDirectory = this.blockBlobContainer.GetDirectoryReference(this.PartitionFolderName);

                if (this.pageBlobContainer == this.blockBlobContainer)
                {
                    this.pageBlobPartitionDirectory = this.blockBlobPartitionDirectory;
                }
                else
                {
                    await this.pageBlobContainer.CreateIfNotExistsAsync();
                    this.pageBlobPartitionDirectory = this.pageBlobContainer.GetDirectoryReference(this.PartitionFolderName);
                }

                this.eventLogCommitBlob = this.blockBlobPartitionDirectory.GetBlockBlobReference(CommitBlobName);

                AzureStorageDevice createDevice(string name) =>
                    new AzureStorageDevice(name, this.blockBlobPartitionDirectory.GetDirectoryReference(name), this.pageBlobPartitionDirectory.GetDirectoryReference(name), this, true);

                var eventLogDevice = createDevice(EventLogBlobName);
                var hybridLogDevice = createDevice(HybridLogBlobName);
                var objectLogDevice = createDevice(ObjectLogBlobName);

                var psfLogDevices = (from groupOrdinal in Enumerable.Range(0, this.PsfGroupCount)
                                     let psfblockDirectory = this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(groupOrdinal))
                                     let psfpageDirectory = this.pageBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(groupOrdinal))
                                     select new AzureStorageDevice(PsfHybridLogBlobName, psfblockDirectory.GetDirectoryReference(PsfHybridLogBlobName), psfpageDirectory.GetDirectoryReference(PsfHybridLogBlobName), this, true)).ToArray();

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

        internal void DisposeDevices()
        {
            Dispose(this.HybridLogDevice);
            Dispose(this.ObjectLogDevice);
            Array.ForEach(this.PsfLogDevices, logDevice => Dispose(logDevice));

            void Dispose(IDevice device)
            {
                this.TraceHelper.FasterStorageProgress($"Disposing Device {device.FileName}");
                device.Dispose();
            }
        }

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

        public static async Task DeleteTaskhubStorageAsync(CloudStorageAccount account, CloudStorageAccount pageBlobAccount, string localFileDirectoryPath, string taskHubName, string pathPrefix)
        {
            var containerName = GetContainerName(taskHubName);

            if (!string.IsNullOrEmpty(localFileDirectoryPath))
            {
                DirectoryInfo di = new DirectoryInfo($"{localFileDirectoryPath}\\{containerName}"); //TODO fine-grained deletion
                if (di.Exists)
                {
                    di.Delete(true);
                }
            }
            else
            {
                async Task DeleteContainerContents(CloudStorageAccount account)
                {
                    CloudBlobClient serviceClient = account.CreateCloudBlobClient();
                    var blobContainer = serviceClient.GetContainerReference(containerName);

                    if (await blobContainer.ExistsAsync())
                    {
                        BlobContinuationToken continuationToken = null;
                        var deletionTasks = new List<Task>();

                        do
                        {
                            var listingResult = await blobContainer.ListBlobsSegmentedAsync(
                                pathPrefix,
                                useFlatBlobListing: true,
                                BlobListingDetails.None, 50, continuationToken, null, null);
                                
                            continuationToken = listingResult.ContinuationToken;
                             
                            foreach (var result in listingResult.Results)
                            {
                                if (result is CloudBlob blob)
                                {
                                    deletionTasks.Add(BlobUtils.ForceDeleteAsync(blob));
                                }
                            }

                            await Task.WhenAll(deletionTasks);
                        }
                        while (continuationToken != null);
                    }

                    // We are not deleting the container itself because it creates problems when trying to recreate
                    // the same container.
                }

                await DeleteContainerContents(account);

                if (pageBlobAccount != account)
                {
                    await DeleteContainerContents(pageBlobAccount);
                }
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

                    if (!this.UseLocalFiles)
                    {
                        this.FaultInjector?.StorageAccess(this, "AcquireLeaseAsync", "AcquireOwnership", this.eventLogCommitBlob.Name);
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

                    this.FaultInjector?.BreakLease(this.eventLogCommitBlob); // during fault injection tests, we don't want to wait

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
                            "CreateCommitLog",
                            "",
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

                                return 1;
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
                catch (Exception ex) when (numAttempts < BlobManager.MaxRetries && BlobUtils.IsTransientStorageError(ex, this.PartitionErrorHandler.Token))
                {
                    if (BlobUtils.IsTimeout(ex))
                    {
                        this.TraceHelper.FasterPerfWarning($"Lease acquisition timed out, retrying now");
                    }
                    else
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.TraceHelper.FasterPerfWarning($"Lease acquisition failed transiently, retrying in {nextRetryIn}");
                        await Task.Delay(nextRetryIn);
                    }
                    continue;
                }
                catch (OperationCanceledException) when (this.PartitionErrorHandler.IsTerminated)
                {
                    throw; // o.k. during termination or shutdown
                }
                catch (Exception e) when (!Utils.IsFatal(e))
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

                if (!this.UseLocalFiles)
                {
                    this.TraceHelper.LeaseProgress($"Renewing lease at {this.leaseTimer.Elapsed.TotalSeconds - this.LeaseDuration.TotalSeconds}s");
                    this.FaultInjector?.StorageAccess(this, "RenewLeaseAsync", "RenewLease", this.eventLogCommitBlob.Name);
                    await this.eventLogCommitBlob.RenewLeaseAsync(acc, this.PartitionErrorHandler.Token)
                        .ConfigureAwait(false);
                    this.TraceHelper.LeaseRenewed(this.leaseTimer.Elapsed.TotalSeconds, this.leaseTimer.Elapsed.TotalSeconds - this.LeaseDuration.TotalSeconds);

                    if (nextLeaseTimer.ElapsedMilliseconds > 2000)
                    {
                        this.TraceHelper.FasterPerfWarning($"RenewLeaseAsync took {nextLeaseTimer.Elapsed.TotalSeconds:F1}s, which is excessive; {this.leaseTimer.Elapsed.TotalSeconds - this.LeaseDuration.TotalSeconds}s past expiry");
                    }
                }

                this.leaseTimer = nextLeaseTimer;
            }
            catch (OperationCanceledException) when (this.PartitionErrorHandler.IsTerminated)
            {
                throw; // o.k. during termination or shutdown
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.TraceHelper.LeaseLost(this.leaseTimer.Elapsed.TotalSeconds, nameof(RenewLeaseTask));
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
                if (!this.UseLocalFiles)
                {
                    try
                    {
                        this.TraceHelper.LeaseProgress("Releasing lease");

                        this.FaultInjector?.StorageAccess(this, "ReleaseLeaseAsync", "ReleaseLease", this.eventLogCommitBlob.Name);
                        AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };

                        await this.eventLogCommitBlob.ReleaseLeaseAsync(
                            accessCondition: acc,
                            options: BlobManager.BlobRequestOptionsDefault,
                            operationContext: null,
                            cancellationToken: this.PartitionErrorHandler.Token).ConfigureAwait(false);

                        this.TraceHelper.LeaseReleased(this.leaseTimer.Elapsed.TotalSeconds);
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

        public async Task RemoveObsoleteCheckpoints()
        {
            if (this.UseLocalFiles)
            {
                //TODO
                return;
            }
            else
            {
                string postFix1 = $"{cprCheckpointPrefix}{this.CheckpointInfo.LogToken.ToString()}/";
                string postFix2 = $"{indexCheckpointPrefix}{this.CheckpointInfo.IndexToken.ToString()}/";

                this.TraceHelper.FasterProgress($"Removing obsolete checkpoints, keeping only {postFix1} and {postFix2}");

                var tasks = new List<Task<(int,int)>>();

                tasks.Add(RemoveObsoleteCheckpoints(this.blockBlobPartitionDirectory.GetDirectoryReference(cprCheckpointPrefix)));
                tasks.Add(RemoveObsoleteCheckpoints(this.blockBlobPartitionDirectory.GetDirectoryReference(indexCheckpointPrefix)));

                if (this.pageBlobPartitionDirectory != this.blockBlobPartitionDirectory)
                {
                    tasks.Add(RemoveObsoleteCheckpoints(this.pageBlobPartitionDirectory.GetDirectoryReference(cprCheckpointPrefix)));
                    tasks.Add(RemoveObsoleteCheckpoints(this.pageBlobPartitionDirectory.GetDirectoryReference(indexCheckpointPrefix)));
                }

                await Task.WhenAll(tasks);

                this.TraceHelper.FasterProgress($"Removed {tasks.Select(t => t.Result.Item1).Sum()} checkpoint directories containing {tasks.Select(t => t.Result.Item2).Sum()} blobs");

                async Task<(int,int)> RemoveObsoleteCheckpoints(CloudBlobDirectory directory)
                {
                    IEnumerable<IListBlobItem> results = null;
                    
                    await this.PerformWithRetriesAsync(
                        BlobManager.AsynchronousStorageWriteMaxConcurrency,
                        true,
                        "CloudBlobDirectory.ListBlobsSegmentedAsync",
                        "RemoveObsoleteCheckpoints",
                        "",
                        directory.Prefix,
                        1000,
                        false,
                        async (numAttempts) =>
                        {
                            var response = await directory.ListBlobsSegmentedAsync(
                              useFlatBlobListing: false,
                              BlobListingDetails.None, 5, null, null, null);
                            results = response.Results.ToList();
                            return results.Count();
                        });

                    var deletionTasks = new List<Task<int>>();

                    foreach (var item in results)
                    {
                        if (item is CloudBlobDirectory cloudBlobDirectory)
                        {
                            if (!cloudBlobDirectory.Prefix.EndsWith(postFix1)
                                && !cloudBlobDirectory.Prefix.EndsWith(postFix2))
                            {
                                deletionTasks.Add(DeleteCheckpointDirectory(cloudBlobDirectory));
                            }
                        }
                    }

                    await Task.WhenAll(deletionTasks);
                    return (deletionTasks.Count, deletionTasks.Select(t => t.Result).Sum());
                }

                async Task<int> DeleteCheckpointDirectory(CloudBlobDirectory directory)
                {
                    BlobContinuationToken continuationToken = null;
                    var deletionTasks = new List<Task>();
                    int count = 0;

                    do
                    {
                        BlobResultSegment listingResult = null;

                        await this.PerformWithRetriesAsync(
                            BlobManager.AsynchronousStorageWriteMaxConcurrency,
                            false,
                            "CloudBlobDirectory.ListBlobsSegmentedAsync",
                            "DeleteCheckpointDirectory",
                            "",
                            directory.Prefix,
                            1000,
                            false,
                            async (numAttempts) =>
                            {
                                var response = await directory.ListBlobsSegmentedAsync(
                                  useFlatBlobListing: true,
                                  BlobListingDetails.None, 5, continuationToken, null, null);
                                listingResult = response;
                                return listingResult.Results.Count();
                            });

                      
                        continuationToken = listingResult.ContinuationToken;

                        foreach (var item in listingResult.Results)
                        {
                            if (item is CloudBlob blob)
                            {
                                count++;
                                deletionTasks.Add(
                                    this.PerformWithRetriesAsync(
                                        BlobManager.AsynchronousStorageWriteMaxConcurrency,
                                        false,
                                        "BlobUtils.ForceDeleteAsync",
                                        "DeleteCheckpointDirectory",
                                        "",
                                        blob.Name,
                                        1000,
                                        false,
                                        async (numAttempts) => (await BlobUtils.ForceDeleteAsync(blob) ? 1 : 0)));
                            }
                        }                               

                        await Task.WhenAll(deletionTasks);
                    }
                    while (continuationToken != null);

                    return count;
                }
            }
        }
    
        #region Blob Name Management

        string GetCheckpointCompletedBlobName() => "last-checkpoint.json";

        const string indexCheckpointPrefix = "index-checkpoints/";

        const string cprCheckpointPrefix = "cpr-checkpoints/";

        string GetIndexCheckpointMetaBlobName(Guid token) => $"{indexCheckpointPrefix}{token}/info.dat";

        (string, string) GetPrimaryHashTableBlobName(Guid token) => ($"{indexCheckpointPrefix}{token}", "ht.dat");

        string GetHybridLogCheckpointMetaBlobName(Guid token) => $"{cprCheckpointPrefix}{token}/info.dat";

        (string, string) GetLogSnapshotBlobName(Guid token) => ($"{cprCheckpointPrefix}{token}", "snapshot.dat");

        (string, string) GetObjectLogSnapshotBlobName(Guid token) => ($"{cprCheckpointPrefix}{token}", "snapshot.obj.dat");

        (string, string) GetDeltaLogSnapshotBlobName(Guid token) => ($"{cprCheckpointPrefix}{token}", "snapshot.delta.dat");

        string GetSingletonsSnapshotBlobName(Guid token) => $"{cprCheckpointPrefix}{token}/singletons.dat";

        #endregion

        #region ILogCommitManager

        void ILogCommitManager.Commit(long beginAddress, long untilAddress, byte[] commitMetadata, long commitNum)
        {
            try
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ILogCommitManager.Commit beginAddress={beginAddress} untilAddress={untilAddress}");

                AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };

                this.PerformWithRetries(
                    false,
                    "CloudBlockBlob.UploadFromByteArray",
                    "WriteCommitLogMetadata",
                    "",
                    this.eventLogCommitBlob.Name,
                    1000,
                    true,
                    (int numAttempts) =>
                    {
                        try
                        {
                            var blobRequestOptions = numAttempts > 2 ? BlobManager.BlobRequestOptionsDefault : BlobManager.BlobRequestOptionsAggressiveTimeout;
                            this.eventLogCommitBlob.UploadFromByteArray(commitMetadata, 0, commitMetadata.Length, acc, blobRequestOptions);
                            return (commitMetadata.Length, true);
                        }
                        catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
                        {
                            // We lost the lease to someone else. Terminate ownership immediately.
                            this.TraceHelper.LeaseLost(this.leaseTimer.Elapsed.TotalSeconds, nameof(ILogCommitManager.Commit));
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
                            return (commitMetadata.Length, false);
                        }
                    });

                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ILogCommitManager.Commit");
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ILogCommitManager.Commit failed");
                throw;
            }
        }


        IEnumerable<long> ILogCommitManager.ListCommits()
        {
            // we only use a single commit file in this implementation
            yield return 0;
        }

        void ILogCommitManager.OnRecovery(long commitNum) 
        { 
            // TODO: make sure our use of single commmit is safe
        }

        void ILogCommitManager.RemoveAllCommits()
        {
            // TODO: make sure our use of single commmit is safe
        }

        void ILogCommitManager.RemoveCommit(long commitNum) 
        {
            // TODO: make sure our use of single commmit is safe
        }

        byte[] ILogCommitManager.GetCommitMetadata(long commitNum)
        {
            try
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ILogCommitManager.GetCommitMetadata (thread={Thread.CurrentThread.ManagedThreadId})");
                AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };
                using var stream = new MemoryStream();

                this.PerformWithRetries(
                   false,
                   "CloudBlockBlob.DownloadToStream",
                   "ReadCommitLogMetadata",
                   "",
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
                           return (stream.Position, true);
                       }
                       catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
                       {
                       // We lost the lease to someone else. Terminate ownership immediately.
                       this.TraceHelper.LeaseLost(this.leaseTimer.Elapsed.TotalSeconds, nameof(ILogCommitManager.GetCommitMetadata));
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
                           return (0, false);
                       }
                   });

                var bytes = stream.ToArray();
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ILogCommitManager.GetCommitMetadata {bytes?.Length ?? null} bytes");
                return bytes.Length == 0 ? null : bytes;
            }
            catch(Exception e)
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ILogCommitManager.GetCommitMetadata failed with {e.GetType().Name}: {e.Message}");
                throw;
            }
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

        void ICheckpointManager.CommitLogIncrementalCheckpoint(Guid logToken, long version, byte[] commitMetadata, DeltaLog deltaLog)
            => this.CommitLogIncrementalCheckpoint(logToken, version, commitMetadata, deltaLog, InvalidPsfGroupOrdinal);

        byte[] ICheckpointManager.GetIndexCheckpointMetadata(Guid indexToken)
            => this.GetIndexCheckpointMetadata(indexToken, InvalidPsfGroupOrdinal);

        byte[] ICheckpointManager.GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta, long recoverTo)
            => this.GetLogCheckpointMetadata(logToken, InvalidPsfGroupOrdinal, deltaLog, scanDelta, recoverTo);

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
            => this.GetIndexDevice(indexToken, InvalidPsfGroupOrdinal);

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
            => this.GetSnapshotLogDevice(token, InvalidPsfGroupOrdinal);

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
            => this.GetSnapshotObjectLogDevice(token, InvalidPsfGroupOrdinal);

        IDevice ICheckpointManager.GetDeltaLogDevice(Guid token)
            => this.GetDeltaLogDevice(token, InvalidPsfGroupOrdinal);

        IEnumerable<Guid> ICheckpointManager.GetIndexCheckpointTokens()
        {
            var indexToken = this.CheckpointInfo.IndexToken;
            this.StorageTracer?.FasterStorageProgress($"StorageOp ICheckpointManager.GetIndexCheckpointTokens indexToken={indexToken}");
            yield return indexToken;
        }

        IEnumerable<Guid> ICheckpointManager.GetLogCheckpointTokens()
        {
            var logToken = this.CheckpointInfo.LogToken;
            this.StorageTracer?.FasterStorageProgress($"StorageOp ICheckpointManager.GetLogCheckpointTokens logToken={logToken}");
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
                    string jsonString;
                    if (this.UseLocalFiles)
                    {
                        jsonString = this.LocalCheckpointManager.GetLatestCheckpointJson();
                    }
                    else
                    {
                        var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                        checkpointCompletedBlob = partDir.GetBlockBlobReference(this.GetCheckpointCompletedBlobName());
                        await this.ConfirmLeaseIsGoodForAWhileAsync();
                        jsonString = await checkpointCompletedBlob.DownloadTextAsync();
                    }

                    // read the fields from the json to update the checkpoint info
                    JsonConvert.PopulateObject(jsonString, isPsf ? this.PsfCheckpointInfos[psfGroupOrdinal] : this.CheckpointInfo);
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
            try
            {
                var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.CommitIndexCheckpoint on {tag}, indexToken={indexToken}");
                var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var metaFileBlob = partDir.GetBlockBlobReference(this.GetIndexCheckpointMetaBlobName(indexToken));

                this.PerformWithRetries(
                 false,
                 "CloudBlockBlob.OpenWrite",
                 "WriteIndexCheckpointMetadata",
                 $"token={indexToken} size={commitMetadata.Length}",
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
                         return (commitMetadata.Length, true);
                     }
                 });

                (isPsf ? this.PsfCheckpointInfos[psfGroupOrdinal] : this.CheckpointInfo).IndexToken = indexToken;
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.CommitIndexCheckpoint from {tag}, target={metaFileBlob.Name}");
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.CommitIndexCheckpoint failed");
                throw;
            }
        }

        internal void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata, int psfGroupOrdinal)
        {
            try
            {
                var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.CommitLogCheckpoint on {tag}, logToken={logToken}");
                var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var metaFileBlob = partDir.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlobName(logToken));

                this.PerformWithRetries(
                    false,
                    "CloudBlockBlob.OpenWrite",
                    "WriteHybridLogCheckpointMetadata",
                    $"token={logToken}",
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
                            return (commitMetadata.Length + 4, true);
                        }
                    });

                (isPsf ? this.PsfCheckpointInfos[psfGroupOrdinal] : this.CheckpointInfo).LogToken = logToken;
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.CommitLogCheckpoint from {tag}, target={metaFileBlob.Name}");
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.CommitLogCheckpoint failed");
                throw;
            }
        }

        internal void CommitLogIncrementalCheckpoint(Guid logToken, long version, byte[] commitMetadata, DeltaLog deltaLog, int indexOrdinal)
        {
            throw new NotImplementedException("incremental checkpointing is not implemented");
        }

        internal byte[] GetIndexCheckpointMetadata(Guid indexToken, int psfGroupOrdinal)
        {
            try
            {
                var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetIndexCommitMetadata on {tag}, indexToken={indexToken}");
                var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var metaFileBlob = partDir.GetBlockBlobReference(this.GetIndexCheckpointMetaBlobName(indexToken));
                byte[] result = null;

                this.PerformWithRetries(
                   false,
                   "CloudBlockBlob.OpenRead",
                   "ReadIndexCheckpointMetadata",
                   "",
                   metaFileBlob.Name,
                   1000,
                   true,
                   (numAttempts) =>
                   {
                       using var blobstream = metaFileBlob.OpenRead();
                       using var reader = new BinaryReader(blobstream);
                       var len = reader.ReadInt32();
                       result = reader.ReadBytes(len);
                       return (len + 4, true);
                   });

                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetIndexCommitMetadata {result?.Length ?? null} bytes from {tag}, target={metaFileBlob.Name}");
                return result;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetIndexCommitMetadata failed");
                throw;
            }
        }

        internal byte[] GetLogCheckpointMetadata(Guid logToken, int psfGroupOrdinal, DeltaLog deltaLog, bool scanDelta, long recoverTo)
        {
            try
            {
                var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetIndexCommitMetadata on {tag}, logToken={logToken}");
                var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var metaFileBlob = partDir.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlobName(logToken));
                byte[] result = null;

                this.PerformWithRetries(
                    false,
                    "CloudBlockBlob.OpenRead",
                    "ReadLogCheckpointMetadata",
                    "",
                    metaFileBlob.Name,
                    1000,
                    true,
                    (numAttempts) =>
                    {
                        using var blobstream = metaFileBlob.OpenRead();
                        using var reader = new BinaryReader(blobstream);
                        var len = reader.ReadInt32();
                        result = reader.ReadBytes(len);
                        return (len + 4, true);
                    });

                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetIndexCommitMetadata {result?.Length ?? null} bytes from {tag}, target={metaFileBlob.Name}");
                return result;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetIndexCommitMetadata failed");
                throw;
            }
        }

        void GetPartitionDirectories(bool isPsf, int psfGroupOrdinal, string path, out CloudBlobDirectory blockBlobDir, out CloudBlobDirectory pageBlobDir)
        {
            var blockPartDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
            blockBlobDir = blockPartDir.GetDirectoryReference(path);
            var pagePartDir = isPsf ? this.pageBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(psfGroupOrdinal)) : this.pageBlobPartitionDirectory;
            pageBlobDir = pagePartDir.GetDirectoryReference(path);
        }

        internal IDevice GetIndexDevice(Guid indexToken, int psfGroupOrdinal)
        {
            try 
            {
                var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetIndexDevice on {tag}, indexToken={indexToken}");
                var (path, blobName) = this.GetPrimaryHashTableBlobName(indexToken);
                this.GetPartitionDirectories(isPsf, psfGroupOrdinal, path, out var blockBlobDir, out var pageBlobDir);
                var device = new AzureStorageDevice(blobName, blockBlobDir, pageBlobDir, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetIndexDevice from {tag}, target={blockBlobDir.Prefix}{blobName}");
                return device;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetIndexDevice failed");
                throw;
            }
        }

        internal IDevice GetSnapshotLogDevice(Guid token, int psfGroupOrdinal)
        {
            try
            {
                var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetSnapshotLogDevice on {tag}, token={token}");
                var (path, blobName) = this.GetLogSnapshotBlobName(token);
                this.GetPartitionDirectories(isPsf, psfGroupOrdinal, path, out var blockBlobDir, out var pageBlobDir);
                var device = new AzureStorageDevice(blobName, blockBlobDir, pageBlobDir, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetSnapshotLogDevice from {tag}, blobDirectory={blockBlobDir} blobName={blobName}");
                return device;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetSnapshotLogDevice failed");
                throw;
            }
        }

        internal IDevice GetSnapshotObjectLogDevice(Guid token, int psfGroupOrdinal)
        {
            try
            {
                var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetSnapshotObjectLogDevice on {tag}, token={token}");
                var (path, blobName) = this.GetObjectLogSnapshotBlobName(token);
                this.GetPartitionDirectories(isPsf, psfGroupOrdinal, path, out var blockBlobDir, out var pageBlobDir);
                var device = new AzureStorageDevice(blobName, blockBlobDir, pageBlobDir, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetSnapshotObjectLogDevice from {tag}, blobDirectory={blockBlobDir} blobName={blobName}");
                return device;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetSnapshotObjectLogDevice failed");
                throw;
            }
        }

        internal IDevice GetDeltaLogDevice(Guid token, int psfGroupOrdinal)
        {
            try
            {
                var (isPsf, tag) = this.IsPsfOrPrimary(psfGroupOrdinal);
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetDeltaLogDevice on {tag}, token={token}");
                var (path, blobName) = this.GetDeltaLogSnapshotBlobName(token);
                this.GetPartitionDirectories(isPsf, psfGroupOrdinal, path, out var blockBlobDir, out var pageBlobDir);
                var device = new AzureStorageDevice(blobName, blockBlobDir, pageBlobDir, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetDeltaLogDevice from {tag}, blobDirectory={blockBlobDir} blobName={blobName}");
                return device;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetDeltaLogDevice failed");
                throw;
            }
        }

#endregion

        internal async Task PersistSingletonsAsync(byte[] singletons, Guid guid)
        {
            if (this.UseLocalFiles)
            {
                var path = Path.Combine(this.LocalCheckpointDirectoryPath, this.GetSingletonsSnapshotBlobName(guid));
                using var filestream = File.OpenWrite(path);
                await filestream.WriteAsync(singletons, 0, singletons.Length);
                await filestream.FlushAsync();
            }
            else
            {
                var singletonsBlob = this.blockBlobPartitionDirectory.GetBlockBlobReference(this.GetSingletonsSnapshotBlobName(guid));
                await this.PerformWithRetriesAsync(
                   BlobManager.AsynchronousStorageWriteMaxConcurrency,
                   false,
                   "CloudBlockBlob.UploadFromByteArrayAsync",
                   "WriteSingletons",
                   "",
                   singletonsBlob.Name,
                   1000 + singletons.Length / 5000,
                   false,
                   async (numAttempts) =>
                   {
                       await singletonsBlob.UploadFromByteArrayAsync(singletons, 0, singletons.Length);
                       return singletons.Length;
                   });
            }
        }

        internal async Task<Stream> RecoverSingletonsAsync()
        {
            if (this.UseLocalFiles)
            {
                var path = Path.Combine(this.LocalCheckpointDirectoryPath, this.GetSingletonsSnapshotBlobName(this.CheckpointInfo.LogToken));
                var stream = File.OpenRead(path);
                return stream;
            }
            else
            {
                var singletonsBlob = this.blockBlobPartitionDirectory.GetBlockBlobReference(this.GetSingletonsSnapshotBlobName(this.CheckpointInfo.LogToken));
                var stream = new MemoryStream();
                await this.PerformWithRetriesAsync(
                    BlobManager.AsynchronousStorageReadMaxConcurrency,
                    true,
                    "CloudBlockBlob.DownloadToStreamAsync",
                    "ReadSingletons",
                    "",
                    singletonsBlob.Name,
                    20000,
                    true,
                    async (numAttempts) =>
                    {
                        stream.Seek(0, SeekOrigin.Begin);
                        await singletonsBlob.DownloadToStreamAsync(stream);
                        return stream.Position;
                    });

                stream.Seek(0, SeekOrigin.Begin);
                return stream;
            }
        }

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
                    "WriteCheckpointMetadata",
                    "",
                    checkpointCompletedBlob.Name,
                    1000,
                    true,
                    async (numAttempts) => 
                    { 
                        await checkpointCompletedBlob.UploadTextAsync(text);
                        return text.Length;
                    });
            }

            // Primary FKV
            {
                var jsonText = JsonConvert.SerializeObject(this.CheckpointInfo, Formatting.Indented);
                if (this.UseLocalFiles)
                    writeLocal(this.LocalCheckpointDirectoryPath, jsonText);
                else
                    await writeBlob(this.blockBlobPartitionDirectory, jsonText);
            }

            // PSFs
            for (var ii = 0; ii < this.PsfLogDevices.Length; ++ii)
            {
                var jsonText = JsonConvert.SerializeObject(this.PsfCheckpointInfos[ii], Formatting.Indented);
                if (this.UseLocalFiles)
                    writeLocal(this.LocalPsfCheckpointDirectoryPath(ii), jsonText);
                else
                    await writeBlob(this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(ii)), jsonText);
            }
        }
 
        #endregion
    }
}
