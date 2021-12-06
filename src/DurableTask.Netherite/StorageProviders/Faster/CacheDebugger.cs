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

    /// <summary>
    /// Records cache and storage management traces for each object. This class is only used for testing and debugging, as it creates lots of overhead.
    /// </summary>
    public class CacheDebugger
    {
        readonly ConcurrentDictionary<TrackedObjectKey, ObjectInfo> Objects = new ConcurrentDictionary<TrackedObjectKey, ObjectInfo>();

        public enum CacheEvent
        {
            // Faster IFunctions
            InitialUpdate,
            InPlaceUpdate,
            CopyUpdate,
            SingleReader,
            SingleReaderPrefetch,
            ConcurrentReader,
            ConcurrentReaderPrefetch,
            SingleWriter,
            ConcurrentWriter,

            // Asynchronous Read Processing
            ReadHit,
            ReadMiss,
            ReadMissComplete,

            // subscriptions to the FASTER log accessor
            Evict,
            EvictTombstone,
            Readonly,
            ReadonlyTombstone,

            // serialization
            SerializeBytes,
            SerializeObject,
            DeserializeBytes,
            DeserializeObject,
        };

        public class ObjectInfo
        {
            public object CurrentValue;
            public int CurrentVersion;
            public List<Entry> CacheEvents;

            public override string ToString()
            {
                return $"Current=v{this.CurrentVersion} CacheEvents={this.CacheEvents.Count}";
            }
        }

        public event Action<string> OnError;

        internal void Subscribe(FASTER.core.LogAccessor<FasterKV.Key, FasterKV.Value> log)
        {
            log.SubscribeEvictions(new EvictionObserver(this));
            log.Subscribe(new ReadonlyObserver(this));
        }

        class EvictionObserver : IObserver<IFasterScanIterator<FasterKV.Key, FasterKV.Value>>
        {
            readonly CacheDebugger cacheDebugger;
            public EvictionObserver(CacheDebugger cacheDebugger)
            {
                this.cacheDebugger = cacheDebugger;
            }

            public void OnCompleted() { }
            public void OnError(Exception error) { }

            public void OnNext(IFasterScanIterator<FasterKV.Key, FasterKV.Value> iterator)
            {
                while (iterator.GetNext(out RecordInfo recordInfo))
                {
                    TrackedObjectKey key = iterator.GetKey().Val;
                    if (!recordInfo.Tombstone)
                    {
                        int version = iterator.GetValue().Version;
                        this.cacheDebugger.Record(key, CacheEvent.Evict, version, null);
                    }
                    else
                    {
                        this.cacheDebugger.Record(key, CacheEvent.EvictTombstone, null, null);
                    }
                }
            }
        }

        class ReadonlyObserver : IObserver<IFasterScanIterator<FasterKV.Key, FasterKV.Value>>
        {
            readonly CacheDebugger cacheDebugger;
            public ReadonlyObserver(CacheDebugger cacheDebugger)
            {
                this.cacheDebugger = cacheDebugger;
            }

            public void OnCompleted() { }
            public void OnError(Exception error) { }

            public void OnNext(IFasterScanIterator<FasterKV.Key, FasterKV.Value> iterator)
            {
                while (iterator.GetNext(out RecordInfo recordInfo))
                {
                    TrackedObjectKey key = iterator.GetKey().Val;
                    if (!recordInfo.Tombstone)
                    {
                        int version = iterator.GetValue().Version;
                        this.cacheDebugger.Record(key, CacheEvent.Readonly, version, null);
                    }
                    else
                    {
                        this.cacheDebugger.Record(key, CacheEvent.ReadonlyTombstone, null, null);
                    }
                }
            }
        }

        public Task CreateTimer(TimeSpan timeSpan)
        {
            return Task.Delay(timeSpan);
        }

        public async ValueTask CheckTiming(Task waitingFor, Task timer, string message)
        {
            var first = await Task.WhenAny(waitingFor, timer);
            if (first == timer)
            {
                this.OnError($"timeout: {message}");
            }
        }

        //public static string StateDescriptor(object o)
        //{
        //    if (o == null)
        //    {
        //        return "null";
        //    }
        //    else if (o is byte[] bytes)
        //    {
        //        return $"byte[{bytes.Length}]";
        //    }
        //    else if (o is HistoryState h)
        //    {
        //        return $"ExecutionId={h.ExecutionId} episode={h.Episode}";
        //    }
        //    else if (o is InstanceState s)
        //    {
        //        return $"lastUpdated={s.OrchestrationState.LastUpdatedTime:o}";
        //    }
        //    else
        //    {
        //        return "INVALID";
        //    }
        //}

        //public static bool StateEquals(object o1, object o2)
        //{
        //    if (o1 == o2)
        //    {
        //        return true;
        //    }
        //    else
        //    {
        //        return StateDescriptor(o1) == StateDescriptor(o2);
        //    }
        //}

        public struct Entry
        {
            public string EventId;
            public CacheEvent CacheEvent;
            public int? Version;

            public override string ToString()
            {
                var sb = new StringBuilder();

                sb.Append(this.CacheEvent.ToString());

                if (this.Version != null)
                {
                    sb.Append('.');
                    sb.Append('v');
                    sb.Append(this.Version.ToString());
                }

                //if (!string.IsNullOrEmpty(this.EventId))
                //{
                //    sb.Append('.');
                //    sb.Append(this.EventId);
                //}

                return sb.ToString();
            }
        }

        internal void Record(TrackedObjectKey key, CacheEvent evt, int? version, string eventId)
        {
            Entry entry = new Entry
            {
                EventId = eventId,
                CacheEvent = evt,
                Version = version,
            };

            this.Objects.AddOrUpdate(
                key,
                key => new ObjectInfo()
                {
                    CurrentValue = null,
                    CacheEvents = new List<Entry>() { entry },
                },
                (key, trace) =>
                {
                    trace.CacheEvents.Add(entry);
                    return trace;
                });
        }

        internal void ConsistentRead(ref TrackedObjectKey key, TrackedObject obj, int version)
        {
            var objectInfo = this.Objects[key];

            if (version != objectInfo.CurrentVersion)
            {
                if (System.Diagnostics.Debugger.IsAttached)
                {
                    System.Diagnostics.Debugger.Break();
                }

                string cacheEvents = string.Join(",", objectInfo.CacheEvents.Select(e => e.ToString()));
                this.OnError($"Read validation failed: expected=v{objectInfo.CurrentVersion} actual=v{version} cacheEvents={cacheEvents}");
            }

            // Caution: obj can be null if version == 0

            //if (!StateEquals(objectInfo.CurrentValue, obj))
            //{
            //    if (System.Diagnostics.Debugger.IsAttached)
            //    {
            //        System.Diagnostics.Debugger.Break();
            //    }

            //    string cacheEvents = string.Join(",", objectInfo.CacheEvents.Select(e => e.ToString()));
            //    this.OnError($"Read validation failed: expected={StateDescriptor(objectInfo.CurrentValue)} actual={StateDescriptor(obj)} cacheEvents={cacheEvents}");
            //}
        }

        internal void ConsistentWrite(ref TrackedObjectKey key, TrackedObject obj, int version)
        {
            this.Objects.AddOrUpdate(
                key,
                key => new ObjectInfo()
                {
                    CurrentValue = obj,
                    CurrentVersion = version,
                    CacheEvents = new List<Entry>(),
                },
                (key, trace) =>
                {
                    trace.CurrentValue = obj;
                    trace.CurrentVersion = version;
                    return trace;
                });
        }

        internal void ConsistentWrite(ref TrackedObjectKey key, object obj, int version)
        {
            if (obj is byte[] bytes)
            {
                this.ConsistentWrite(ref key, (TrackedObject) DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes), version);
            }
            else
            {
                this.ConsistentWrite(ref key, obj as TrackedObject, version);
            }
        }

        public IEnumerable<string> Dump()
        {
            foreach (var kvp in this.Objects)
            {
                yield return $"{kvp.Key,-25} {string.Join(",", kvp.Value.CacheEvents.Select(e => e.ToString()))}";
            }
        }
    }
}
