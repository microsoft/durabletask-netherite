// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using Microsoft.Extensions.Logging;

    class FasterTraceHelper
    {
        readonly ILogger logger;
        readonly LogLevel logLevelLimit;
        readonly ILogger performanceLogger;
        readonly string account;
        readonly string taskHub;
        readonly int partitionId;

        public FasterTraceHelper(ILogger logger, LogLevel logLevelLimit, ILogger performanceLogger, uint partitionId, string storageAccountName, string taskHubName)
        {
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            this.performanceLogger = performanceLogger;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.partitionId = (int) partitionId;
        }

        public bool IsTracingAtMostDetailedLevel => this.logLevelLimit == LogLevel.Trace;

        // ----- faster storage layer events

        public void FasterStoreCreated((long,int) inputQueuePosition, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Created Store, inputQueuePosition={inputQueuePosition}.{inputQueueBatchPosition} latencyMs={latencyMs}", this.partitionId, inputQueuePosition.Item1, inputQueuePosition.Item2, latencyMs);
                EtwSource.Log.FasterStoreCreated(this.account, this.taskHub, this.partitionId, inputQueuePosition.Item1, inputQueuePosition.Item2, latencyMs, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }
        public void FasterCheckpointStarted(Guid checkpointId, string details, string storeStats, long commitLogPosition, (long, int) inputQueuePosition)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Started Checkpoint {checkpointId}, details={details}, storeStats={storeStats}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}.{inputQueueBatchPosition}", this.partitionId, checkpointId, details, storeStats, commitLogPosition, inputQueuePosition.Item1, inputQueuePosition.Item2);
                EtwSource.Log.FasterCheckpointStarted(this.account, this.taskHub, this.partitionId, checkpointId, details, storeStats, commitLogPosition, inputQueuePosition.Item1, inputQueuePosition.Item2, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterCheckpointPersisted(Guid checkpointId, string details, long commitLogPosition, (long,int) inputQueuePosition, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Persisted Checkpoint {checkpointId}, details={details}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}.{inputQueueBatchPosition} latencyMs={latencyMs}", this.partitionId, checkpointId, details, commitLogPosition, inputQueuePosition.Item1, inputQueuePosition.Item2, latencyMs);
                EtwSource.Log.FasterCheckpointPersisted(this.account, this.taskHub, this.partitionId, checkpointId, details, commitLogPosition, inputQueuePosition.Item1, inputQueuePosition.Item2, latencyMs, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }

            if (latencyMs > 10000)
            {
                this.FasterPerfWarning($"Persisting the checkpoint {checkpointId} took {(double)latencyMs / 1000}s, which is excessive; checkpointId={checkpointId} commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}");
            }
        }

        public void FasterLogPersisted(long commitLogPosition, long numberEvents, long sizeInBytes, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger.LogDebug("Part{partition:D2} Persisted Log, commitLogPosition={commitLogPosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes} latencyMs={latencyMs}", this.partitionId, commitLogPosition, numberEvents, sizeInBytes, latencyMs);
                EtwSource.Log.FasterLogPersisted(this.account, this.taskHub, this.partitionId, commitLogPosition, numberEvents, sizeInBytes, latencyMs, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }

            if (latencyMs > 10000)
            {
                this.FasterPerfWarning($"Persisting the log took {(double)latencyMs / 1000}s, which is excessive; commitLogPosition={commitLogPosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes}");
            }
        }

        public void FasterPerfWarning(string details)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                this.performanceLogger.LogWarning("Part{partition:D2} Performance issue detected: {details}", this.partitionId, details);
                EtwSource.Log.FasterPerfWarning(this.account, this.taskHub, this.partitionId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterCheckpointLoaded(long commitLogPosition, (long,int) inputQueuePosition, string storeStats, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Loaded Checkpoint, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}.{inputQueueBatchPosition}  storeStats={storeStats} latencyMs={latencyMs}", this.partitionId, commitLogPosition, inputQueuePosition.Item1, inputQueuePosition.Item2, storeStats, latencyMs);
                EtwSource.Log.FasterCheckpointLoaded(this.account, this.taskHub, this.partitionId, commitLogPosition, inputQueuePosition.Item1, inputQueuePosition.Item2, storeStats, latencyMs, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterLogReplayed(long commitLogPosition, (long,int) inputQueuePosition, long numberEvents, long sizeInBytes, string storeStats, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Replayed CommitLog, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}.{inputQueueBatchPosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes} storeStats={storeStats} latencyMs={latencyMs}", this.partitionId, commitLogPosition, inputQueuePosition.Item1, inputQueuePosition.Item2, numberEvents, sizeInBytes, storeStats, latencyMs);
                EtwSource.Log.FasterLogReplayed(this.account, this.taskHub, this.partitionId, commitLogPosition, inputQueuePosition.Item1, inputQueuePosition.Item2, numberEvents, sizeInBytes, storeStats, latencyMs, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterStorageError(string context, Exception exception)
        {
            if (this.logLevelLimit <= LogLevel.Error)
            {
                this.logger.LogError("Part{partition:D2} !!! Faster Storage Error : {context} : {exception}", this.partitionId, context, exception);
                EtwSource.Log.FasterStorageError(this.account, this.taskHub, this.partitionId, context, exception.ToString(), TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterCacheSizeMeasured(int numPages, long numRecords, long sizeInBytes, long gcMemory, long processMemory, long discrepancy, double elapsedMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Measured CacheSize numPages={numPages} numRecords={numRecords} sizeInBytes={sizeInBytes} gcMemory={gcMemory} processMemory={processMemory} discrepancy={discrepancy} elapsedMs={elapsedMs:F2}", this.partitionId, numPages, numRecords, sizeInBytes, gcMemory, processMemory, discrepancy, elapsedMs);
                EtwSource.Log.FasterCacheSizeMeasured(this.account, this.taskHub, this.partitionId, numPages, numRecords, sizeInBytes, gcMemory, processMemory, discrepancy, elapsedMs, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger.LogDebug("Part{partition:D2} {details}", this.partitionId, details);
                EtwSource.Log.FasterProgress(this.account, this.taskHub, this.partitionId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterStorageProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Trace)
            {
                this.logger.LogTrace("Part{partition:D2} {details}", this.partitionId, details);
                EtwSource.Log.FasterStorageProgress(this.account, this.taskHub, this.partitionId, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterAzureStorageAccessCompleted(string intent, long size, string operation, string target, double latency, int attempt)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger.LogDebug("Part{partition:D2} storage access completed intent={intent} size={size} operation={operation} target={target} latency={latency} attempt={attempt}", 
                    this.partitionId, intent, size, operation, target, latency, attempt);
                EtwSource.Log.FasterAzureStorageAccessCompleted(this.account, this.taskHub, this.partitionId, intent, size, operation, target, latency, attempt, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public enum CompactionProgress { Skipped, Started, Completed };

        public void FasterCompactionProgress(CompactionProgress progress, string operation, long begin, long safeReadOnly, long tail, long minimalSize, long compactionAreaSize, double elapsedMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Compaction {progress} operation={operation} begin={begin} safeReadOnly={safeReadOnly} tail={tail} minimalSize={minimalSize} compactionAreaSize={compactionAreaSize} elapsedMs={elapsedMs}", this.partitionId, progress, operation, begin, safeReadOnly, tail, minimalSize, compactionAreaSize, elapsedMs);
                EtwSource.Log.FasterCompactionProgress(this.account, this.taskHub, this.partitionId, progress.ToString(), operation, begin, safeReadOnly, tail, minimalSize, compactionAreaSize, elapsedMs, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            } 
        }

        // ----- lease management events

        public void LeaseAcquired()
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} PartitionLease acquired", this.partitionId);
                EtwSource.Log.FasterLeaseAcquired(this.account, this.taskHub, this.partitionId, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void LeaseRenewed(double elapsedSeconds, double timing)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger.LogDebug("Part{partition:D2} PartitionLease renewed after {elapsedSeconds:F2}s timing={timing:F2}s", this.partitionId, elapsedSeconds, timing);
                EtwSource.Log.FasterLeaseRenewed(this.account, this.taskHub, this.partitionId, elapsedSeconds, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void LeaseReleased(double elapsedSeconds)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} PartitionLease released after {elapsedSeconds:F2}s", this.partitionId, elapsedSeconds);
                EtwSource.Log.FasterLeaseReleased(this.account, this.taskHub, this.partitionId, elapsedSeconds, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void LeaseLost(double elapsedSeconds, string operation)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                this.logger.LogWarning("Part{partition:D2} PartitionLease lost after {elapsedSeconds:F2}s in {operation}", this.partitionId, elapsedSeconds, operation);
                EtwSource.Log.FasterLeaseLost(this.account, this.taskHub, this.partitionId, operation, elapsedSeconds, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }

        public void LeaseProgress(string operation)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger.LogDebug("Part{partition:D2} PartitionLease progress: {operation}", this.partitionId, operation);
                EtwSource.Log.FasterLeaseProgress(this.account, this.taskHub, this.partitionId, operation, TraceUtils.AppName, TraceUtils.ExtensionVersion);
            }
        }
    }
}
