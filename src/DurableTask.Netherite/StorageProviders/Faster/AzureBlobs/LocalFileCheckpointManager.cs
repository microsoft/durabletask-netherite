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
        readonly LocalCheckpointManager localCheckpointManager;
        readonly string checkpointCompletedFilename;
        readonly bool isIndex;

        internal LocalFileCheckpointManager(CheckpointInfo ci, bool isIndex, string checkpointDir, string checkpointCompletedBlobName)
        {
            this.checkpointInfo = ci;
            this.isIndex = isIndex;
            this.localCheckpointManager = new LocalCheckpointManager(checkpointDir);
            this.checkpointCompletedFilename = Path.Combine(checkpointDir, checkpointCompletedBlobName);
        }

        void ICheckpointManager.InitializeIndexCheckpoint(Guid indexToken)
            => this.localCheckpointManager.InitializeIndexCheckpoint(indexToken);

        void ICheckpointManager.InitializeLogCheckpoint(Guid logToken)
            => this.localCheckpointManager.InitializeLogCheckpoint(logToken);

        void ICheckpointManager.CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
        {
            this.localCheckpointManager.CommitIndexCheckpoint(indexToken, commitMetadata);

            if (this.isIndex)
            {
                this.checkpointInfo.SecondaryIndexIndexToken = indexToken;
            }
            else
            {
                this.checkpointInfo.IndexToken = indexToken;
            }
        }

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            this.localCheckpointManager.CommitLogCheckpoint(logToken, commitMetadata);

            if (this.isIndex)
            {
                this.checkpointInfo.SecondaryIndexLogToken = logToken;
            }
            else
            {
                this.checkpointInfo.LogToken = logToken;
            }
        }

        void ICheckpointManager.CommitLogIncrementalCheckpoint(Guid logToken, int version, byte[] commitMetadata, DeltaLog deltaLog)
        {
            // TODO: verify implementation of CommitLogIncrementalCheckpoint
            this.localCheckpointManager.CommitLogIncrementalCheckpoint(logToken, version, commitMetadata, deltaLog);

            if (this.isIndex)
            {
                this.checkpointInfo.SecondaryIndexLogToken = logToken;
            }
            else
            {
                this.checkpointInfo.LogToken = logToken;
            }
        }

        byte[] ICheckpointManager.GetIndexCheckpointMetadata(Guid indexToken)
            => this.localCheckpointManager.GetIndexCheckpointMetadata(indexToken);

        byte[] ICheckpointManager.GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog)
            => this.localCheckpointManager.GetLogCheckpointMetadata(logToken, deltaLog);

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
            if (!this.isIndex)
            {
                yield return this.checkpointInfo.IndexToken;
            }
            else if (this.checkpointInfo.SecondaryIndexIndexToken.HasValue)
            {
                yield return this.checkpointInfo.SecondaryIndexIndexToken.Value;
            }
        }

        IEnumerable<Guid> ICheckpointManager.GetLogCheckpointTokens()
        {
            if (!this.isIndex)
            {
                yield return this.checkpointInfo.LogToken;
            }
            else if (this.checkpointInfo.SecondaryIndexLogToken.HasValue)
            {
                yield return this.checkpointInfo.SecondaryIndexLogToken.Value;
            }
        }

        void ICheckpointManager.PurgeAll()
            => this.localCheckpointManager.PurgeAll();

        public void OnRecovery(Guid indexToken, Guid logToken)
            => this.localCheckpointManager.OnRecovery(indexToken, logToken);

        void IDisposable.Dispose()
            => this.localCheckpointManager.Dispose();
    }
}
