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
            Recovery,
            SizeCheck,
        };

        public class ObjectInfo
        {
            public int CurrentVersion;
            public List<Entry> CacheEvents;
            public long? Size = 0;
            
            public override string ToString()
            {
                return $"Current=v{this.CurrentVersion} Size={this.Size} CacheEvents={this.CacheEvents.Count}";
            }

            public string PrintCacheEvents() => string.Join(",", this.CacheEvents.Select(e => e.ToString()));
        }

        public event Action<string> OnError;

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
            Entry entry = new Entry
            {
                EventId = eventId,
                CacheEvent = evt,
                Version = version,
                Address = address,
            };

            this.Objects.AddOrUpdate(
                key,
                key => new ObjectInfo()
                {
                    CacheEvents = new List<Entry>() { entry },
                },
                (key, info) =>
                {
                    info.CacheEvents.Add(entry);
                    return info;
                });
        }

        internal void UpdateTrackedObjectSize(long delta, TrackedObjectKey key, long address)
        {
            Entry entry = new Entry
            {          
                CacheEvent = CacheEvent.TrackSize,
                Delta = delta,
                Address = address,
            };

            this.Objects.AddOrUpdate(
               key,
               key => new ObjectInfo()
               {
                   CacheEvents = new List<Entry>() { entry },
                   Size = delta,
               },
               (key, info) =>
               {
                   info.CacheEvents.Add(entry);
                   info.Size += delta;
                   return info;
               });
        }

        internal bool CheckSize(TrackedObjectKey key, long actual, string desc)
        {
            long? expected = this.Objects[key].Size;

            if (expected == null)
            {
                // after recovery, we don't know the size. Record it now.
                this.Objects[key].Size = actual;
            }
            else
            {
                if (expected != actual)
                {
                    this.Fail($"Size tracking is not accurate expected={expected} actual={actual} desc={desc}", key);
                    return false;
                }
            }
            this.Objects[key].CacheEvents.Add(new Entry { CacheEvent = CacheEvent.SizeCheck, Delta = actual });
            return true;
        }

        internal void OnRecovery()
        {
            // reset all size tracking
            foreach (var info in this.Objects.Values)
            {
                info.CacheEvents.Add(new Entry() { CacheEvent = CacheEvent.Recovery });
                info.Size = null;
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

            var objectInfo = this.Objects[key];
            this.OnError($"{message} key={key} cacheEvents={objectInfo.PrintCacheEvents()}");
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

        public IEnumerable<string> Dump()
        {
            foreach (var kvp in this.Objects)
            {
                yield return $"{kvp.Key,-25} {string.Join(",", kvp.Value.CacheEvents.Select(e => e.ToString()))}";
            }
        }
    }
}
