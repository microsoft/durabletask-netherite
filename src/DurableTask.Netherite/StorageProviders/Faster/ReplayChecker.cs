// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using FASTER.core;
    using Newtonsoft.Json;

    /// <summary>
    /// Validates the replay, by maintaining an ongoing checkpoint and confirming the commutative diagram
    /// serialize(new-state) = serialize(deserialize(old-state) + event)
    /// This class is only used for testing and debugging, as it creates lots of overhead.
    /// </summary>
    class ReplayChecker
    {
        readonly ConcurrentDictionary<Partition, Info> partitionInfo;
        readonly TestHooks testHooks;

        public ReplayChecker(TestHooks testHooks)
        {
            this.testHooks = testHooks;
            this.partitionInfo = new ConcurrentDictionary<Partition, Info>();
        }

        class Info
        {
            public Partition Partition;
            public Dictionary<TrackedObjectKey, string> Store;
            public long CommitLogPosition;
            public long InputQueuePosition;
            public EffectTracker EffectTracker;
        }

        readonly JsonSerializerSettings settings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.Auto,
        };

        string Serialize(TrackedObject trackedObject)
        { 
            string serialized =  trackedObject == null ? "null" : JsonConvert.SerializeObject(trackedObject, typeof(TrackedObject), Formatting.Indented, this.settings);
            if (trackedObject is SessionsState)
            {
                serialized = serialized.Replace("\"IsPlayed\": true", "\"IsPlayed\": false");
            }
            else if (trackedObject is HistoryState)
            {
                serialized = serialized.Replace("\"IsPlayed\": false", "\"IsPlayed\": true");
            }
            return serialized;
        }

        TrackedObject DeserializeTrackedObject(string content, TrackedObjectKey key)
            => (content != null) ? (TrackedObject) JsonConvert.DeserializeObject(content, this.settings) : TrackedObjectKey.Factory(key);

        string Serialize(PartitionUpdateEvent partitionUpdateEvent)
            => JsonConvert.SerializeObject(partitionUpdateEvent, typeof(PartitionUpdateEvent), Formatting.Indented, this.settings);

        PartitionUpdateEvent DeserializePartitionUpdateEvent(string content)
            => (PartitionUpdateEvent) JsonConvert.DeserializeObject(content, this.settings);

        public void PartitionStarting(Partition partition, TrackedObjectStore store, long CommitLogPosition, long InputQueuePosition)
        {
            var info = new Info()
            {
                Partition = partition,
                Store = new Dictionary<TrackedObjectKey, string>(),
                CommitLogPosition = CommitLogPosition,
                InputQueuePosition = InputQueuePosition,
            };

            this.partitionInfo[partition] = info;

            store.EmitCurrentState((TrackedObjectKey key, TrackedObject value) =>
            {
                info.Store.Add(key, this.Serialize(value));
            });

            info.EffectTracker = new ReplayCheckEffectTracker(this, info);
        }

        public async Task CheckUpdate(Partition partition, PartitionUpdateEvent partitionUpdateEvent, TrackedObjectStore store)
        {
            var info = this.partitionInfo[partition];

            System.Diagnostics.Trace.WriteLine($"REPLAYCHECK STARTED {partitionUpdateEvent}");

            var eventForReplay = this.DeserializePartitionUpdateEvent(this.Serialize(partitionUpdateEvent));
            eventForReplay.NextCommitLogPosition = partitionUpdateEvent.NextCommitLogPosition;
            await info.EffectTracker.ProcessUpdate(eventForReplay);

            // check that the two match, generate error message and fix difference otherwise

            HashSet<TrackedObjectKey> NotVisited = new HashSet<TrackedObjectKey>(info.Store.Keys);

            store.EmitCurrentState((TrackedObjectKey key, TrackedObject value) =>
            {
                NotVisited.Remove(key);
                string expected = this.Serialize(value);

                if (!info.Store.TryGetValue(key, out var replayed))
                {
                    this.testHooks.Error(this.GetType().Name, $"key={key}\nexpected={expected}\nreplayed=absent");
                    info.Store[key] = expected;
                }
                if (expected != replayed)   
                {
                    var expectedlines = TraceUtils.GetLines(expected).ToArray();
                    var replayedlines = TraceUtils.GetLines(replayed).ToArray();
                    string expectedline = "";
                    string replayedline = "";
                    int i = 0;
                    for (; i < Math.Max(expectedlines.Length, replayedlines.Length); i++)
                    {
                        expectedline = i < expectedlines.Length ? expectedlines[i] : "absent";
                        replayedline = i < replayedlines.Length ? replayedlines[i] : "absent";
                        if (expectedline != replayedline)
                        {
                            break;
                        }
                    }
                    this.testHooks.Error(this.GetType().Name, $"key={key} line={i}\nexpectedline={expectedline}\nreplayedline={replayedline}\nexpected={expected}\nreplayed={replayed} ");
                    info.Store[key] = expected;
                }
            });

            foreach(var key in NotVisited)
            {
                string val = info.Store[key];
                this.testHooks.Error(this.GetType().Name, $"key={key}\nexpected=absent\nreplayed={val}");
                info.Store.Remove(key);
            }

            System.Diagnostics.Trace.WriteLine("REPLAYCHECK DONE");
        }

        public void PartitionStopped(Partition partition)
        {
            this.partitionInfo.TryRemove(partition, out _);
        }

        class ReplayCheckEffectTracker : EffectTracker
        {
            readonly ReplayChecker replayChecker;
            readonly ReplayChecker.Info info;

            public ReplayCheckEffectTracker(ReplayChecker replayChecker, ReplayChecker.Info info)
            {
                this.replayChecker = replayChecker;
                this.info = info;
                this.IsReplaying = true;
            }

            public override Partition Partition => this.info.Partition;

            public override EventTraceHelper EventTraceHelper => null;

            public override EventTraceHelper EventDetailTracer => null;

            public override uint PartitionId => this.info.Partition.PartitionId;

            public override double CurrentTimeMs => 0;
 
            public override ValueTask ApplyToStore(TrackedObjectKey key, EffectTracker tracker)
            {
                this.info.Store.TryGetValue(key, out string content);
                var trackedObject = this.replayChecker.DeserializeTrackedObject(content, key);
                trackedObject.Partition = this.Partition;
                tracker.ProcessEffectOn(trackedObject);
                content = this.replayChecker.Serialize(trackedObject);
                this.info.Store[key] = content;
                return default;
            }

            public override ValueTask RemoveFromStore(IEnumerable<TrackedObjectKey> keys)
            {
                foreach (var key in keys)
                {
                    this.info.Store.Remove(key);
                }
                return default;
            }

            public override (long, long) GetPositions()
            {
                return (this.info.CommitLogPosition, this.info.InputQueuePosition);
            }

            public override void Assert(bool condition)
            {
                if (!condition)
                {
                    this.replayChecker.testHooks.Error(this.replayChecker.GetType().Name, "assertion failed");
                }
            }

            protected override void HandleError(string where, string message, Exception e, bool terminatePartition, bool reportAsWarning)
            {
                this.replayChecker.testHooks.Error(this.replayChecker.GetType().Name, $"{where}: {message} {e}");
            }       
        }
    }
}
