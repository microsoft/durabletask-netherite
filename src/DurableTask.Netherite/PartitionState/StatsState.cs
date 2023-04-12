// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using DurableTask.Netherite.Scaling;

    [DataContract]
    class StatsState : TrackedObject
    {
        [DataMember]
        public long InstanceCount { get; set; }

        [DataMember]
        public SortedSet<string> InstanceIds { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Stats);

        [IgnoreDataMember]
        public bool HasInstanceIds => this.InstanceIds != null;

        public override string ToString()
        {
            return $"Stats (InstanceCount={this.InstanceCount})";
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Instances = this.InstanceCount;
        }

        void ValidateInvariant(Partition partition, PartitionUpdateEvent evt = null)
        {
            if (this.InstanceIds != null && this.InstanceCount != this.InstanceIds.Count)
            {
                // this should not happen, but we have seen it happen. To help fix it, we generate
                // a warning here, and keep going.

                partition.ErrorHandler.HandleError(
                    "StatsState", 
                    $"Invariant violated: InstanceCount={this.InstanceCount} InstanceIds.Count={this.InstanceIds.Count} Position={evt?.NextCommitLogPosition ?? 0}", 
                    null, 
                    terminatePartition: false, 
                    reportAsWarning: true);

                this.InstanceCount = this.InstanceIds.Count; // use the set of instance ids as the authoritative source
            }
        }

        public override void OnFirstInitialization(Partition partition)
        {
            // indexing the keys 
            if (partition.Settings.KeepInstanceIdsInMemory)
            {
                this.InstanceIds = new SortedSet<string>();
            }

            this.ValidateInvariant(partition);
        }

        public override void Process(RecoveryCompleted evt, EffectTracker effects)
        {
            if (this.HasInstanceIds && !evt.KeepInstanceIdsInMemory)
            {
                this.InstanceIds = null; // remove the index
            }
            else if (!this.HasInstanceIds && evt.KeepInstanceIdsInMemory && !effects.IsReplaying)
            {
                // TODO: kick off a background task to rebuild the index
            }

            this.ValidateInvariant(effects.Partition, evt);
        }

        public override void Process(CreationRequestReceived evt, EffectTracker effects)
        {
            this.InstanceCount++;

            if (this.InstanceIds != null)
            {
                lock (this.InstanceIds)
                {
                    this.InstanceIds.Add(evt.InstanceId);
                }
            }

            this.ValidateInvariant(effects.Partition, evt);
        }

        public override void Process(BatchProcessed evt, EffectTracker effects)
        {
            if (!evt.DeleteInstance)
            {
                this.InstanceCount++;
            }
            else
            {
                this.InstanceCount--;
            }

            if (this.InstanceIds != null)
            {
                lock (this.InstanceIds)
                {
                    if (!evt.DeleteInstance)
                    {
                        this.InstanceIds.Add(evt.InstanceId);
                    }
                    else
                    {
                        this.InstanceIds.Remove(evt.InstanceId);
                    }
                }
            }

            this.ValidateInvariant(effects.Partition, evt);
        }

        public override void Process(PurgeBatchIssued evt, EffectTracker effects)
        {
            this.InstanceCount -= evt.Purged.Count;

            if (this.InstanceIds != null)
            {
                lock (this.InstanceIds)
                {
                    foreach (var key in evt.Purged)
                    {
                        this.InstanceIds.Remove(key);
                    }
                }
            }

            this.ValidateInvariant(effects.Partition, evt);
        }

        public override void Process(DeletionRequestReceived evt, EffectTracker effects)
        {
            this.InstanceCount--;

            if (this.InstanceIds != null)
            {
                lock (this.InstanceIds)
                {
                    this.InstanceIds.Remove(evt.InstanceId);
                }
            }

            this.ValidateInvariant(effects.Partition, evt);
        }

        // called by query
        public IEnumerator<string> GetEnumerator(string prefix, string from)
        {
            int pageSize = 500;

            Func<string, bool> predicate =
                string.IsNullOrEmpty(prefix) ? ((s) => true) : ((s) => s.StartsWith(prefix));

            while (true)
            {
                var chunk = GetChunk();

                foreach (var s in chunk)
                {
                    yield return s;
                }

                if (chunk.Count < 500)
                {
                    yield break;
                }

                from = chunk[chunk.Count - 1];
            }

            List<string> GetChunk()
            {
                lock (this.InstanceIds)
                {
                    if (string.IsNullOrEmpty(from))
                    {
                        return this.InstanceIds.Where(predicate).Take(pageSize).ToList();
                    }
                    else
                    {
                        return this.InstanceIds.GetViewBetween(from, this.InstanceIds.Max).Where(s => s != from).Where(predicate).Take(pageSize).ToList();
                    }
                }
            }
        }
    }
}
