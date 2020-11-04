// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Faster
{
    using System;
    using Microsoft.Extensions.Logging;

    class FasterTraceHelper
    {
        readonly ILogger logger;
        readonly LogLevel logLevelLimit;
        readonly string account;
        readonly string taskHub;
        readonly int partitionId;

        public FasterTraceHelper(ILogger logger, LogLevel logLevelLimit, uint partitionId, string storageAccountName, string taskHubName)
        {
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.partitionId = (int) partitionId;
        }

        EtwSource etwLogTrace => (this.logLevelLimit <= LogLevel.Trace) ? EtwSource.Log : null;
        EtwSource etwLogDebug => (this.logLevelLimit <= LogLevel.Debug) ? EtwSource.Log : null;
        EtwSource etwLogInformation => (this.logLevelLimit <= LogLevel.Information) ? EtwSource.Log : null;
        EtwSource etwLogWarning => (this.logLevelLimit <= LogLevel.Warning) ? EtwSource.Log : null;
        EtwSource etwLogError => (this.logLevelLimit <= LogLevel.Error) ? EtwSource.Log : null;

        public bool IsTracingAtMostDetailedLevel => this.logLevelLimit == LogLevel.Trace;

        // ----- faster storage provider events

        public void FasterStoreCreated(long inputQueuePosition, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Created Store, inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", this.partitionId, inputQueuePosition, latencyMs);
                this.etwLogInformation?.FasterStoreCreated(this.account, this.taskHub, this.partitionId, inputQueuePosition, latencyMs, TraceUtils.ExtensionVersion);
            }
        }
        public void FasterCheckpointStarted(Guid checkpointId, string reason, string storeStats, long commitLogPosition, long inputQueuePosition)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Started Checkpoint {checkpointId}, reason={reason}, storeStats={storeStats}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}", this.partitionId, checkpointId, reason, storeStats, commitLogPosition, inputQueuePosition);
                this.etwLogInformation?.FasterCheckpointStarted(this.account, this.taskHub, this.partitionId, checkpointId, reason, storeStats, commitLogPosition, inputQueuePosition, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterCheckpointPersisted(Guid checkpointId, string reason, long commitLogPosition, long inputQueuePosition, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Persisted Checkpoint {checkpointId}, reason={reason}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", this.partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition, latencyMs);
                this.etwLogInformation?.FasterCheckpointPersisted(this.account, this.taskHub, this.partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition, latencyMs, TraceUtils.ExtensionVersion);
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
                this.etwLogDebug?.FasterLogPersisted(this.account, this.taskHub, this.partitionId, commitLogPosition, numberEvents, sizeInBytes, latencyMs, TraceUtils.ExtensionVersion);
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
                this.logger.LogWarning("Part{partition:D2} Performance issue detected: {details}", this.partitionId, details);
                this.etwLogDebug?.FasterPerfWarning(this.account, this.taskHub, this.partitionId, details, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterCheckpointLoaded(long commitLogPosition, long inputQueuePosition, string storeStats, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Loaded Checkpoint, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}  storeStats={storeStats} latencyMs={latencyMs}", this.partitionId, commitLogPosition, inputQueuePosition, storeStats, latencyMs);
                this.etwLogInformation?.FasterCheckpointLoaded(this.account, this.taskHub, this.partitionId, commitLogPosition, inputQueuePosition, storeStats, latencyMs, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterLogReplayed(long commitLogPosition, long inputQueuePosition, long numberEvents, long sizeInBytes, string storeStats, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Replayed CommitLog, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes} storeStats={storeStats} latencyMs={latencyMs}", this.partitionId, commitLogPosition, inputQueuePosition, numberEvents, sizeInBytes, storeStats, latencyMs);
                this.etwLogInformation?.FasterLogReplayed(this.account, this.taskHub, this.partitionId, commitLogPosition, inputQueuePosition, numberEvents, sizeInBytes, storeStats, latencyMs, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterStorageError(string context, Exception exception)
        {
            if (this.logLevelLimit <= LogLevel.Error)
            {
                this.logger.LogError("Part{partition:D2} !!! Faster Storage Error : {context} : {exception}", this.partitionId, context, exception);
                this.etwLogError?.FasterStorageError(this.account, this.taskHub, this.partitionId, context, exception.ToString(), TraceUtils.ExtensionVersion);
            }
        }

        public void FasterBlobStorageWarning(string context, string blobName, Exception exception)
        {
            if (this.logLevelLimit <= LogLevel.Error)
            {
                this.logger.LogError(exception, "Part{partition:D2} !!! Faster Blob Storage error : {context} blobName={blobName} {exception}", this.partitionId, context, blobName, exception);
                this.etwLogError?.FasterBlobStorageWarning(this.account, this.taskHub, this.partitionId, context, blobName ?? string.Empty, exception?.ToString() ?? string.Empty, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterBlobStorageError(string context, string blobName, Exception exception)
        {
            if (this.logLevelLimit <= LogLevel.Error)
            {
                this.logger.LogError(exception, "Part{partition:D2} !!! Faster Blob Storage error : {context} blobName={blobName} {exception}", this.partitionId, context, blobName, exception);
                this.etwLogError?.FasterBlobStorageError(this.account, this.taskHub, this.partitionId, context, blobName ?? string.Empty, exception?.ToString() ?? string.Empty, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger.LogDebug("Part{partition:D2} {details}", this.partitionId, details);
                this.etwLogDebug?.FasterProgress(this.account, this.taskHub, this.partitionId, details, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterStorageProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Trace)
            {
                this.logger.LogTrace("Part{partition:D2} {details}", this.partitionId, details);
                this.etwLogTrace?.FasterStorageProgress(this.account, this.taskHub, this.partitionId, details, TraceUtils.ExtensionVersion);
            }
        }

        // ----- lease management events

        public void LeaseAcquired()
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Acquired lease", this.partitionId);
                this.etwLogInformation?.FasterLeaseAcquired(this.account, this.taskHub, this.partitionId, TraceUtils.ExtensionVersion);
            }
        }

        public void LeaseReleased()
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} Released lease", this.partitionId);
                this.etwLogInformation?.FasterLeaseReleased(this.account, this.taskHub, this.partitionId, TraceUtils.ExtensionVersion);
            }
        }

        public void LeaseLost(string operation)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                this.logger.LogWarning("Part{partition:D2} Lease lost in {operation}", this.partitionId, operation);
                this.etwLogWarning?.FasterLeaseLost(this.account, this.taskHub, this.partitionId, operation, TraceUtils.ExtensionVersion);
            }
        }

        public void LeaseProgress(string operation)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger.LogDebug("Part{partition:D2} Lease progress: {operation}", this.partitionId, operation);
                this.etwLogDebug?.FasterLeaseProgress(this.account, this.taskHub, this.partitionId, operation, TraceUtils.ExtensionVersion);
            }
        }
    }
}
