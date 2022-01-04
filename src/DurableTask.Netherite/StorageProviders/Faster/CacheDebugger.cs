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
    class CacheDebugger
    {
        readonly TestHooks testHooks;
        readonly ConcurrentDictionary<TrackedObjectKey, ObjectInfo> Objects = new ConcurrentDictionary<TrackedObjectKey, ObjectInfo>();

        public CacheDebugger(TestHooks testHooks)
        {
            this.testHooks = testHooks;
        }

        public enum CacheEvent
        {
            // reads and RMWs on the main session
            StartingRead,
            PendingRead,
            CompletedRead,
            StartingRMW,
            PendingRMW,
            CompletedRMW,

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

            // explicit failure
            Fail,
        };

        public class ObjectInfo
        {
            public int CurrentVersion;
            public List<Entry> CacheEvents;
            
            public override string ToString()
            {
                return $"Current=v{this.CurrentVersion} CacheEvents={this.CacheEvents.Count}";
            }

            public string PrintCacheEvents() => string.Join(",", this.CacheEvents.Select(e => e.ToString()));
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
                this.testHooks.Error(this.GetType().Name, $"timeout: {message}");
            }
        }

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
                    CacheEvents = new List<Entry>() { entry },
                },
                (key, trace) =>
                {
                    trace.CacheEvents.Add(entry);
                    return trace;
                });
        }

        internal void Fail(string message)
        {
            this.testHooks.Error(this.GetType().Name, message);
        }

        internal void Fail(string message, TrackedObjectKey key)
        {
            this.Record(key, CacheEvent.Fail, null, null);

            var objectInfo = this.Objects[key];
            this.testHooks.Error(this.GetType().Name, $"{message} cacheEvents={objectInfo.PrintCacheEvents()}");
        }

        internal void ValidateObjectVersion(FasterKV.Value val, TrackedObjectKey key)
        {
            int? VersionOfObject()
            {
                if (val.Val == null)
                {
                    return 0;
                }
                else if (val.Val is TrackedObject o)
                {
                    return o.Version;
                }
                else if (val.Val is byte[] bytes)
                {
                    return DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes).Version;
                }
                else
                {
                    return null;
                }
            }

            int? versionOfObject = VersionOfObject();

            if (val.Version != versionOfObject)
            {
                var objectInfo = this.Objects[key];
                this.Fail($"incorrect version: reference=v{val.Version} actual=v{versionOfObject} obj={val.Val} cacheEvents={objectInfo.PrintCacheEvents()}");
            }
        }

        internal void CheckVersionConsistency(ref TrackedObjectKey key, TrackedObject obj, int? version)
        {
            var objectInfo = this.Objects[key];

            if (version != null && version.Value != objectInfo.CurrentVersion)
            {
                this.Fail($"Read validation on version failed: expected=v{objectInfo.CurrentVersion} actual=v{version} obj={obj} cacheEvents={objectInfo.PrintCacheEvents()}");
            }

            if ((obj?.Version ?? 0) != objectInfo.CurrentVersion)
            {
                this.Fail($"Read validation on object failed: expected=v{objectInfo.CurrentVersion} actual=v{obj?.Version ?? 0} obj={obj} cacheEvents={objectInfo.PrintCacheEvents()}");
            }
        }

        internal void UpdateReferenceValue(ref TrackedObjectKey key, TrackedObject obj, int version)
        {
            this.Objects.AddOrUpdate(
                key,
                key => new ObjectInfo()
                {
                    CurrentVersion = version,
                    CacheEvents = new List<Entry>(),
                },
                (key, trace) =>
                {
                    trace.CurrentVersion = version;
                    return trace;
                });

            if (obj != null)
            {
                obj.Version = version;
            }
        }

        internal void UpdateReferenceValue(ref TrackedObjectKey key, object obj, int version)
        {
            if (obj is byte[] bytes)
            {
                this.UpdateReferenceValue(ref key, (TrackedObject) DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes), version);
            }
            else
            {
                this.UpdateReferenceValue(ref key, obj as TrackedObject, version);
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
