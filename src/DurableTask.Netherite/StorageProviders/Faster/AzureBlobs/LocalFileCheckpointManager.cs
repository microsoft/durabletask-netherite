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
    using System.IO;
    using FASTER.core;
    using Newtonsoft.Json;

    class LocalFileCheckpointManager : ICheckpointManager
    {
        readonly CheckpointInfo checkpointInfo;
        readonly LocalCheckpointManager localCheckpointManager;
        readonly string checkpointCompletedFilename;

        internal LocalFileCheckpointManager(CheckpointInfo ci, string checkpointDir, string checkpointCompletedBlobName)
        {
            this.checkpointInfo = ci;
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
            this.checkpointInfo.IndexToken = indexToken;
        }

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            this.localCheckpointManager.CommitLogCheckpoint(logToken, commitMetadata);
            this.checkpointInfo.LogToken = logToken;
        }
        byte[] ICheckpointManager.GetIndexCheckpointMetadata(Guid indexToken)
            => this.localCheckpointManager.GetIndexCheckpointMetadata(indexToken);

        byte[] ICheckpointManager.GetLogCheckpointMetadata(Guid logToken)
                 => this.localCheckpointManager.GetLogCheckpointMetadata(logToken);

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
            => this.localCheckpointManager.GetIndexDevice(indexToken);

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
            => this.localCheckpointManager.GetSnapshotLogDevice(token);

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
            => this.localCheckpointManager.GetSnapshotObjectLogDevice(token);

        bool GetLatestCheckpoint(out Guid indexToken, out Guid logToken)
        {
            if (!File.Exists(this.checkpointCompletedFilename))
            {
                indexToken = default;
                logToken = default;
                return false;
            }

            var jsonString = File.ReadAllText(this.checkpointCompletedFilename);
            this.checkpointInfo.CopyFrom(JsonConvert.DeserializeObject<CheckpointInfo>(jsonString));

            indexToken = this.checkpointInfo.IndexToken;
            logToken = this.checkpointInfo.LogToken;
            return indexToken != default && logToken != default;
        }

        IEnumerable<Guid> ICheckpointManager.GetIndexCheckpointTokens()
        {
            if (this.GetLatestCheckpoint(out Guid indexToken, out Guid _))
            {
                yield return indexToken;
            }
        }

        IEnumerable<Guid> ICheckpointManager.GetLogCheckpointTokens()
        {
            if (this.GetLatestCheckpoint(out Guid _, out Guid logToken))
            {
                yield return logToken;
            }
        }

        void ICheckpointManager.PurgeAll()
            => this.localCheckpointManager.PurgeAll();

        void IDisposable.Dispose()
            => this.localCheckpointManager.Dispose();
    }
}
