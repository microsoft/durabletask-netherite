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
        readonly ConcurrentDictionary<TrackedObjectKey, CacheTrace> CacheTraces = new ConcurrentDictionary<TrackedObjectKey, CacheTrace>();

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
            
            // Faster serializer
            Serialize,
            Deserialize,

            // Hybrid Log events
            ReadOnly, 
            Evict 
        };

        public class CacheTrace
        {
            public List<Entry> Objects;

            public override string ToString()
            {
                return $"Count={this.Objects.Count}";
            }
        }

        public struct Entry
        {
            public DateTime Timestamp;
            public CacheEvent CacheEvent;

            public override string ToString()
            {
                return $"{this.Timestamp:o} {this.CacheEvent}";
            }
        }

        internal void Record(ref TrackedObjectKey key, CacheEvent evt, DateTime? timestamp = null)
        {
            Entry entry = new Entry
            {
                Timestamp = timestamp ?? DateTime.UtcNow,
                CacheEvent = evt,
            };
            
            this.CacheTraces.AddOrUpdate(
                key,
                key => new CacheTrace()
                {
                    Objects = new List<Entry>() { entry },
                },
                (key, trace) =>
                {
                    trace.Objects.Add(entry);
                    return trace;
                });
        }

        public IEnumerable<string> Dump()
        {
            foreach (var kvp in this.CacheTraces)
            {
                yield return $"{kvp.Key,-25} {string.Join(",", kvp.Value.Objects.Select(e => e.CacheEvent.ToString()))}";
            }
        }
    }
}
