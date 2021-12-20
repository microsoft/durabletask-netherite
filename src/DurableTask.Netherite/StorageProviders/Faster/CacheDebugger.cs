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

        internal MemoryTracker MemoryTracker { get; set; }
 
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
            PostInitialUpdate,
            InPlaceUpdate,
            CopyUpdate,
            PostCopyUpdate,
            SingleReader,
            SingleReaderPrefetch,
            SingleWriter,
            ConcurrentReader,
            ConcurrentReaderPrefetch,
            PostSingleWriter,
            ConcurrentWriter,
            ConcurrentDeleter,
            PostSingleDeleter,

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

            // tracking adjustment
            TrackSize,

            // other events
            Fail,
            Reset,
            SizeCheck,
        };

        public class ObjectInfo
        {
            public int CurrentVersion;
            public ConcurrentQueue<Entry> CacheEvents;
            public long Size = 0;
            public int? PendingRMW;
            
            public override string ToString()
            {
                return $"Current=v{this.CurrentVersion} Size={this.Size} CacheEvents={this.CacheEvents.Count}";
            }

            public string PrintCacheEvents() => string.Join(",", this.CacheEvents.Select(e => e.ToString()));
        }

        public event Action<string> OnError;

        internal ObjectInfo GetObjectInfo(TrackedObjectKey key)
        {
            return this.Objects.AddOrUpdate(
                key,
                key => new ObjectInfo()
                {
                    CacheEvents = new ConcurrentQueue<Entry>(),
                },
                (key, info) =>
                {
                    return info;
                });
        }

        public struct Entry
        {
            public string EventId;
            public CacheEvent CacheEvent;
            public int? Version;
            public long Delta;
            public long Address;

            public override string ToString()
            {
                var sb = new StringBuilder();

                if (this.CacheEvent == CacheEvent.SizeCheck)
                {
                    sb.Append('✓');
                    sb.Append(this.Delta);
                }
                else
                {
                    sb.Append(this.CacheEvent.ToString());

                    if (this.Version != null)
                    {
                        sb.Append('.');
                        sb.Append('v');
                        sb.Append(this.Version.ToString());
                    }

                    if (this.CacheEvent == CacheEvent.TrackSize)
                    {
                        if (this.Delta >= 0)
                        {
                            sb.Append('+');
                        }
                        sb.Append(this.Delta);
                    }


                    if (this.Address != 0)
                    {
                        sb.Append('@');
                        sb.Append(this.Address.ToString("x"));
                    }

                    //if (!string.IsNullOrEmpty(this.EventId))
                    //{
                    //    sb.Append('@');
                    //    sb.Append(this.EventId);
                    //}

                }
                return sb.ToString();
            }
        }

        internal void Record(TrackedObjectKey key, CacheEvent evt, int? version, string eventId, long address)
        {
            var info = this.GetObjectInfo(key);
            info.CacheEvents.Enqueue(new Entry
            {
                EventId = eventId,
                CacheEvent = evt,
                Version = version,
                Address = address,
            });

            switch(evt)
            {
                case CacheEvent.StartingRMW:
                case CacheEvent.PendingRMW:
                    info.PendingRMW = info.CurrentVersion;
                    break;

                case CacheEvent.CompletedRMW:
                    if (info.CurrentVersion != info.PendingRMW + 1)
                    {
                        this.Fail("RMW completed without correctly updating the object", key);
                    }
                    info.PendingRMW = null;
                    break;

                default:
                    break;
            }
        }

        internal void UpdateTrackedObjectSize(long delta, TrackedObjectKey key, long address)
        {
            var info = this.GetObjectInfo(key);
            info.Size += delta;
            info.CacheEvents.Enqueue(new Entry
            {
                CacheEvent = CacheEvent.TrackSize,
                Delta = delta,
                Address = address,
            });
        }

        internal bool CheckSize(TrackedObjectKey key, long actual, string desc)
        {
            var info = this.GetObjectInfo(key);
            long expected = info.Size;
            if (expected != actual)
            {
                this.Fail($"Size tracking is not accurate expected={expected} actual={actual} desc={desc}", key);
                return false;
            }
            info.CacheEvents.Enqueue(new Entry { CacheEvent = CacheEvent.SizeCheck, Delta = actual });
            return true;
        }

        internal void UpdateSize(TrackedObjectKey key, long delta)
        {
            var info = this.GetObjectInfo(key);
            info.Size += delta;
        }

        internal void Reset()
        {
            // reset all size tracking
            foreach (var info in this.Objects.Values)
            {
                info.CacheEvents.Enqueue(new Entry() { CacheEvent = CacheEvent.Reset });
                info.Size = 0;
            }
        }

        internal void Fail(string message)
        {
            if (System.Diagnostics.Debugger.IsAttached)
            {
                System.Diagnostics.Debugger.Break();
            }

            this.OnError(message);
        }

        internal void Fail(string message, TrackedObjectKey key)
        {
            this.Record(key, CacheEvent.Fail, null, null, 0);

            if (System.Diagnostics.Debugger.IsAttached)
            {
                System.Diagnostics.Debugger.Break();
            }

            var info = this.GetObjectInfo(key);
            this.OnError($"{message} key={key} cacheEvents={info.PrintCacheEvents()}");
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
                var info = this.GetObjectInfo(key);
                this.Fail($"incorrect version: reference=v{val.Version} actual=v{versionOfObject} obj={val.Val} cacheEvents={info.PrintCacheEvents()}");
            }
        }

        internal void CheckVersionConsistency(ref TrackedObjectKey key, TrackedObject obj, int? version)
        {
            var info = this.GetObjectInfo(key);

            if (version != null && version.Value != info.CurrentVersion)
            {
                this.Fail($"Read validation on version failed: expected=v{info.CurrentVersion} actual=v{version} obj={obj} cacheEvents={info.PrintCacheEvents()}");
            }

            if ((obj?.Version ?? 0) != info.CurrentVersion)
            {
                this.Fail($"Read validation on object failed: expected=v{info.CurrentVersion} actual=v{obj?.Version ?? 0} obj={obj} cacheEvents={info.PrintCacheEvents()}");
            }
        }

        internal void UpdateReferenceValue(ref TrackedObjectKey key, TrackedObject obj, int version)
        {
            var info = this.GetObjectInfo(key);
            info.CurrentVersion = version; 
            if (obj != null)
            {
                obj.Version = version;
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
