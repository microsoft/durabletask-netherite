// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

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
        };

        public class ObjectInfo
        {
            public object CurrentValue;
            public List<Entry> CacheEvents;

            public override string ToString()
            {
                return $"Current={StateDescriptor(this.CurrentValue)} CacheEvents={this.CacheEvents.Count}";
            }
        }

        public event Action<string> OnError;

        public static string StateDescriptor(object o)
        {
            if (o == null)
            {
                return "null";
            }
            else if (o is byte[] bytes)
            {
                return $"byte[{bytes.Length}]";
            }
            else if (o is HistoryState h)
            {
                return $"ExecutionId={h.ExecutionId} episode={h.Episode}";
            }
            else if (o is InstanceState s)
            {
                return $"lastUpdated={s.OrchestrationState.LastUpdatedTime:o}";
            }
            else
            {
                return "INVALID";
            }
        }

        public static bool StateEquals(object o1, object o2)
        {
            if (o1 == o2)
            {
                return true;
            }
            else
            {
                return StateDescriptor(o1) == StateDescriptor(o2);
            }
        }

        public struct Entry
        {
            public string EventId;
            public CacheEvent CacheEvent;

            public override string ToString()
            {
                if (string.IsNullOrEmpty(this.EventId))
                {
                    return $"{this.CacheEvent}";
                }
                else
                {
                    return $"{this.CacheEvent}.{this.EventId}";
                }
            }
        }

        internal void Record(ref TrackedObjectKey key, CacheEvent evt, string eventId)
        {
            Entry entry = new Entry
            {
                EventId = eventId,
                CacheEvent = evt,
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

        internal void ConsistentRead(ref TrackedObjectKey key, TrackedObject obj)
        {
            var objectInfo = this.Objects[key];

            if (!StateEquals(objectInfo.CurrentValue, obj))
            {
                if (System.Diagnostics.Debugger.IsAttached)
                {
                    System.Diagnostics.Debugger.Break();
                }

                string cacheEvents = string.Join(",", objectInfo.CacheEvents.Select(e => e.ToString()));
                this.OnError($"Read validation failed: expected={StateDescriptor(objectInfo.CurrentValue)} actual={StateDescriptor(obj)} cacheEvents={cacheEvents}");
            }
        }

        internal void ConsistentWrite(ref TrackedObjectKey key, TrackedObject obj)
        {
            this.Objects.AddOrUpdate(
                key,
                key => new ObjectInfo()
                {
                    CurrentValue = obj,
                    CacheEvents = new List<Entry>(),
                },
                (key, trace) =>
                {
                    trace.CurrentValue = obj;
                    return trace;
                });
        }

        internal void ConsistentWrite(ref TrackedObjectKey key, object obj)
        {
            if (obj is byte[] bytes)
            {
                this.ConsistentWrite(ref key, (TrackedObject) DurableTask.Netherite.Serializer.DeserializeTrackedObject(bytes));
            }
            else
            {
                this.ConsistentWrite(ref key, obj as TrackedObject);
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
