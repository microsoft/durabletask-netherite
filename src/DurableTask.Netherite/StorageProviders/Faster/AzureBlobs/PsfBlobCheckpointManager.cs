// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using FASTER.core;

    class PsfBlobCheckpointManager : ICheckpointManager
    {
        readonly BlobManager blobManager;
        readonly int groupOrdinal;

        internal PsfBlobCheckpointManager(BlobManager blobMan, int groupOrd)
        {
            this.blobManager = blobMan;
            this.groupOrdinal = groupOrd;
        }

        void ICheckpointManager.InitializeIndexCheckpoint(Guid indexToken)
        { } // there is no need to create empty directories in a blob container

        void ICheckpointManager.InitializeLogCheckpoint(Guid logToken)
        { } // there is no need to create empty directories in a blob container

        void ICheckpointManager.CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
            => this.blobManager.CommitIndexCheckpoint(indexToken, commitMetadata, this.groupOrdinal);

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
            => this.blobManager.CommitLogCheckpoint(logToken, commitMetadata, this.groupOrdinal);

        void ICheckpointManager.CommitLogIncrementalCheckpoint(Guid logToken, int version, byte[] commitMetadata, DeltaLog deltaLog)
            => this.blobManager.CommitLogIncrementalCheckpoint(logToken, version, commitMetadata, deltaLog, this.groupOrdinal);

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
            => this.blobManager.GetIndexDevice(indexToken, this.groupOrdinal);

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
            => this.blobManager.GetSnapshotLogDevice(token, this.groupOrdinal);

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
            => this.blobManager.GetSnapshotObjectLogDevice(token, this.groupOrdinal);

        IDevice ICheckpointManager.GetDeltaLogDevice(Guid token)
            => this.blobManager.GetDeltaLogDevice(token, this.groupOrdinal);

        byte[] ICheckpointManager.GetIndexCheckpointMetadata(Guid indexToken)
            => this.blobManager.GetIndexCheckpointMetadata(indexToken, this.groupOrdinal);

        byte[] ICheckpointManager.GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog)
            => this.blobManager.GetLogCheckpointMetadata(logToken, this.groupOrdinal, deltaLog);

        IEnumerable<Guid> ICheckpointManager.GetIndexCheckpointTokens()
        {
            var indexToken = this.blobManager.PsfCheckpointInfos[this.groupOrdinal].IndexToken;
            yield return indexToken;
        }

        IEnumerable<Guid> ICheckpointManager.GetLogCheckpointTokens()
        {
            var logToken = this.blobManager.PsfCheckpointInfos[this.groupOrdinal].LogToken;
            yield return logToken;
        }

        public void PurgeAll() { /* TODO */ }

        public void OnRecovery(Guid indexToken, Guid logToken)
            => this.blobManager.OnRecovery(indexToken, logToken);

        public void Dispose() { }
    }
}
