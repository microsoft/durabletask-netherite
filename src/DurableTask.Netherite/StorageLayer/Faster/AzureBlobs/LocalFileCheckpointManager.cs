// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using FASTER.core;
    using Newtonsoft.Json;

    class LocalFileCheckpointManager : ICheckpointManager
    {
        readonly CheckpointInfo checkpointInfo;
        readonly ICheckpointManager localCheckpointManager;
        readonly string checkpointCompletedFilename;

        internal LocalFileCheckpointManager(CheckpointInfo ci, string checkpointDir, string checkpointCompletedBlobName)
        {
            this.checkpointInfo = ci;
            this.localCheckpointManager = new DeviceLogCommitCheckpointManager
                (new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(
                        new DirectoryInfo(checkpointDir).FullName), removeOutdated: true);
            this.checkpointCompletedFilename = Path.Combine(checkpointDir, checkpointCompletedBlobName);
        }

        void ICheckpointManager.InitializeIndexCheckpoint(Guid indexToken)
            => this.localCheckpointManager.InitializeIndexCheckpoint(indexToken);

        void ICheckpointManager.InitializeLogCheckpoint(Guid logToken)
            => this.localCheckpointManager.InitializeLogCheckpoint(logToken);

        void ICheckpointManager.CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
        {
            this.localCheckpointManager.CommitIndexCheckpoint(indexToken, commitMetadata);
            this.checkpointInfo.IndexToken = indexToken;
        }

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            this.localCheckpointManager.CommitLogCheckpoint(logToken, commitMetadata);
            this.checkpointInfo.LogToken = logToken;
        }

        void ICheckpointManager.CommitLogIncrementalCheckpoint(Guid logToken, long version, byte[] commitMetadata, DeltaLog deltaLog)
        {
            throw new NotImplementedException("incremental checkpointing is not implemented");
        }

        byte[] ICheckpointManager.GetIndexCheckpointMetadata(Guid indexToken)
            => this.localCheckpointManager.GetIndexCheckpointMetadata(indexToken);

        byte[] ICheckpointManager.GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta, long recoverTo)
            => this.localCheckpointManager.GetLogCheckpointMetadata(logToken, deltaLog, scanDelta, recoverTo);

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
            => this.localCheckpointManager.GetIndexDevice(indexToken);

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
            => this.localCheckpointManager.GetSnapshotLogDevice(token);

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
            => this.localCheckpointManager.GetSnapshotObjectLogDevice(token);

        IDevice ICheckpointManager.GetDeltaLogDevice(Guid token)
           => this.localCheckpointManager.GetDeltaLogDevice(token);

        internal string GetLatestCheckpointJson() => File.ReadAllText(this.checkpointCompletedFilename); 

        IEnumerable<Guid> ICheckpointManager.GetIndexCheckpointTokens()
        {
            var indexToken = this.checkpointInfo.IndexToken;
            yield return indexToken;
        }

        IEnumerable<Guid> ICheckpointManager.GetLogCheckpointTokens()
        {
            var logToken = this.checkpointInfo.LogToken;
            yield return logToken;
        }

        void ICheckpointManager.Purge(Guid guid)
            => this.localCheckpointManager.Purge(guid);

        void ICheckpointManager.PurgeAll()
            => this.localCheckpointManager.PurgeAll();

        public void OnRecovery(Guid indexToken, Guid logToken)
            => this.localCheckpointManager.OnRecovery(indexToken, logToken);

        void IDisposable.Dispose()
            => this.localCheckpointManager.Dispose();

        public void CheckpointVersionShift(long oldVersion, long newVersion)
        {
            throw new NotImplementedException();
        }
    }
}
