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
    using Newtonsoft.Json.Linq;
    using Newtonsoft.Json.Serialization;

    /// <summary>
    /// Validates the replay, by maintaining an ongoing checkpoint and confirming the commutative diagram
    /// serialize(new-state) = serialize(deserialize(old-state) + event)
    /// This class is only used for testing and debugging, as it creates lots of overhead.
    /// </summary>
    public class ReplayChecker
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
            if (trackedObject == null)
            {
                return "null";
            }
            else
            {
                JObject jObject = JObject.FromObject(trackedObject, JsonSerializer.Create(this.settings));

                // for the checking to work correctly, we must edit the serialized state as follows:
                // - order all json properties, otherwise nondeterminism causes false errors
                // - modify isPlayed to true (on HistoryState) or false (on SessionsState) to avoid errors due to racing mutations

                bool? fixIsPlayed;
                if (trackedObject is SessionsState)
                {
                    fixIsPlayed = false;
                }
                else if (trackedObject is HistoryState)
                {
                    fixIsPlayed = true;
                }
                else
                {
                    fixIsPlayed = null;
                }

                RecursivelyEdit(jObject);

                return jObject.ToString(Formatting.Indented);

                void RecursivelyEdit(JObject jObj)
                {
                    var children = jObj.Properties().OrderBy(p => p.Name).ToList();
                    foreach (var prop in children)
                    {
                        prop.Remove();
                    }
                    foreach (var prop in children)
                    {
                        jObj.Add(prop);

                        if (prop.Value is JObject o1)
                        {
                            RecursivelyEdit(o1);
                        }
                        else if (prop.Value is JArray)
                        {
                            var numProperties = prop.Value.Count();
                            for (int i = 0; i < numProperties; i++)
                            {
                                if (prop.Value[i] is JObject o2)
                                {
                                    RecursivelyEdit(o2);
                                }
                            }
                        }
                        else if (fixIsPlayed.HasValue && prop.Name == "IsPlayed")
                        {
                            prop.Value = (JToken)fixIsPlayed.Value;
                        }
                    }
                }
            }
        }

        TrackedObject DeserializeTrackedObject(string content, TrackedObjectKey key)
        {
            if (content == "null")
            {
                return null;
            }
            else
            {
                var trackedObject = TrackedObjectKey.Factory(key);
                JsonConvert.PopulateObject(content, trackedObject, this.settings);
                return trackedObject;
            }
        }

        string Serialize(PartitionUpdateEvent partitionUpdateEvent)
            => JsonConvert.SerializeObject(partitionUpdateEvent, typeof(PartitionUpdateEvent), Formatting.Indented, this.settings);

        PartitionUpdateEvent DeserializePartitionUpdateEvent(string content)
            => (PartitionUpdateEvent) JsonConvert.DeserializeObject(content, this.settings);

        internal void PartitionStarting(Partition partition, TrackedObjectStore store, long CommitLogPosition, long InputQueuePosition)
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

        internal async Task CheckUpdate(Partition partition, PartitionUpdateEvent partitionUpdateEvent, TrackedObjectStore store)
        {
            var info = this.partitionInfo[partition];
            var cacheDebugger = partition.Settings.TestHooks?.CacheDebugger;

            partition.EventTraceHelper.TraceEventProcessingDetail($"REPLAYCHECK-STARTED {partitionUpdateEvent.NextCommitLogPosition}");
            int errors = 0;

            string serializedEvent = this.Serialize(partitionUpdateEvent);
            var eventForReplay = this.DeserializePartitionUpdateEvent(serializedEvent);
            eventForReplay.NextCommitLogPosition = partitionUpdateEvent.NextCommitLogPosition;
            await info.EffectTracker.ProcessUpdate(eventForReplay);

            // check that the two match, generate error message and fix difference otherwise

            HashSet<TrackedObjectKey> NotVisited = new HashSet<TrackedObjectKey>(info.Store.Keys);

            store.EmitCurrentState((TrackedObjectKey key, TrackedObject value) =>
            {
                NotVisited.Remove(key);
                string current = this.Serialize(value);

                if (!info.Store.TryGetValue(key, out var replayed))
                {
                    this.testHooks.Error(this.GetType().Name, $"Part{partition.PartitionId:D2} pos={partitionUpdateEvent.NextCommitLogPosition} key={key}"
                        + $"\ncurrent={current}\nreplayed=absent\nevent={serializedEvent}");
                    info.Store[key] = current;
                }
                if (current != replayed)
                {
                    var currentlines = TraceUtils.GetLines(current).ToArray();
                    var replayedlines = TraceUtils.GetLines(replayed).ToArray();
                    string currentline = "";
                    string replayedline = "";
                    int i = 0;
                    for (; i < Math.Max(currentlines.Length, replayedlines.Length); i++)
                    {
                        currentline = i < currentlines.Length ? currentlines[i] : "absent";
                        replayedline = i < replayedlines.Length ? replayedlines[i] : "absent";
                        if (currentline != replayedline)
                        {
                            break;
                        }
                    }
                    this.testHooks.Error(this.GetType().Name, $"Part{partition.PartitionId:D2} pos={partitionUpdateEvent.NextCommitLogPosition} key={key}"
                        + $"\nline={i}\ncurrentline={currentline}\nreplayedline={replayedline}\ncurrent={current}\nreplayed={replayed}\nevent={serializedEvent}");
                    info.Store[key] = current;
                    errors++;
                }
            });

            
            foreach (var key in NotVisited)
            {
                if (!key.IsSingleton)
                {
                    cacheDebugger?.CheckIteratorAbsence(key);
                }

                string val = info.Store[key];
                this.testHooks.Error(this.GetType().Name, $"Part{partition.PartitionId:D2} pos={partitionUpdateEvent.NextCommitLogPosition} key={key}"
                        + $"\ncurrent=absent\nreplayed={val}\nevent={serializedEvent}");
                info.Store.Remove(key);
                errors++;
            }

            if (errors == 0)
            {
                partition.EventTraceHelper.TraceEventProcessingDetail($"REPLAYCHECK-SUCCEEDED {partitionUpdateEvent.NextCommitLogPosition}");
            }
            else
            {
                partition.EventTraceHelper.TraceEventProcessingDetail($"REPLAYCHECK-FAILED {partitionUpdateEvent.NextCommitLogPosition} ({errors} ERRORS)");
            }
        }
         
        internal void PartitionStopped(Partition partition)
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

            public override EventTraceHelper EventTraceHelper => this.info.Partition.EventTraceHelper;

            public override EventTraceHelper EventDetailTracer => this.info.Partition.EventDetailTracer;

            public override uint PartitionId => this.info.Partition.PartitionId;

            public override double CurrentTimeMs => 0;
 
            public override ValueTask ApplyToStore(TrackedObjectKey key, EffectTracker tracker)
            {                
                // retrieve the previously stored state, if present
                TrackedObject trackedObject = null;
                if (this.info.Store.TryGetValue(key, out string content))
                {
                    trackedObject = this.replayChecker.DeserializeTrackedObject(content, key);
                }

                // initialize the tracked object before applying the effect
                trackedObject ??= TrackedObjectKey.Factory(key); 
                trackedObject.Partition = this.Partition;

                // apply the effect using our special tracker that suppresses side effects
                tracker.ProcessEffectOn(trackedObject);

                // store the result back, to reuse on the next update
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

            public override void Assert(bool condition, string message)
            {
                if (!condition)
                {
                    this.replayChecker.testHooks.Error(this.replayChecker.GetType().Name, $"assertion failed: {message}");
                }
            }

            protected override void HandleError(string where, string message, Exception e, bool terminatePartition, bool reportAsWarning)
            {
                this.replayChecker.testHooks.Error(this.replayChecker.GetType().Name, $"{where}: {message} {e}");
            }       
        }
    }
}
