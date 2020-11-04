// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    /// <summary>
    /// Represents a key used to identify <see cref="TrackedObject"/> instances.
    /// </summary>
    struct TrackedObjectKey 
    {
        public TrackedObjectType ObjectType;
        public string InstanceId;

        public TrackedObjectKey(TrackedObjectType objectType) { this.ObjectType = objectType; this.InstanceId = null; }
        public TrackedObjectKey(TrackedObjectType objectType, string instanceId) { this.ObjectType = objectType; this.InstanceId = instanceId; }

        public enum TrackedObjectType
        {
            // singletons
            Activities,
            Dedup,
            Outbox,
            Reassembly,
            Sessions,
            Timers,
            Prefetch,
            Queries,

            // non-singletons
            History,
            Instance,
        }

        public static Dictionary<TrackedObjectType, Type> TypeMap = new Dictionary<TrackedObjectType, Type>()
        {
            { TrackedObjectType.Activities, typeof(ActivitiesState) },
            { TrackedObjectType.Dedup, typeof(DedupState) },
            { TrackedObjectType.Outbox, typeof(OutboxState) },
            { TrackedObjectType.Reassembly, typeof(ReassemblyState) },
            { TrackedObjectType.Sessions, typeof(SessionsState) },
            { TrackedObjectType.Timers, typeof(TimersState) },
            { TrackedObjectType.Prefetch, typeof(PrefetchState) },
            { TrackedObjectType.Queries, typeof(QueriesState) },
            
            // non-singletons
            { TrackedObjectType.History, typeof(HistoryState) },
            { TrackedObjectType.Instance, typeof(InstanceState) },
        };

        public static bool IsSingletonType(TrackedObjectType t) => (int) t < (int) TrackedObjectType.History;

        public bool IsSingleton => IsSingletonType(this.ObjectType);

        public static int Compare(ref TrackedObjectKey key1, ref TrackedObjectKey key2)
        {
            int result = key1.ObjectType.CompareTo(key2.ObjectType);
            return result == 0 ? key1.InstanceId.CompareTo(key2.InstanceId) : result;
        }

        public class Comparer : IComparer<TrackedObjectKey>
        {
            public int Compare(TrackedObjectKey x, TrackedObjectKey y) => TrackedObjectKey.Compare(ref x, ref y); 
        }

        public override int GetHashCode()
        {
            return (this.InstanceId?.GetHashCode() ?? 0) + (int) this.ObjectType;
        }

        public override bool Equals(object obj)
        {
            return (obj is TrackedObjectKey other && this.ObjectType == other.ObjectType && this.InstanceId == other.InstanceId);
        }

        // convenient constructors for singletons

        public static TrackedObjectKey Activities = new TrackedObjectKey() { ObjectType = TrackedObjectType.Activities };
        public static TrackedObjectKey Dedup = new TrackedObjectKey() { ObjectType = TrackedObjectType.Dedup };
        public static TrackedObjectKey Outbox = new TrackedObjectKey() { ObjectType = TrackedObjectType.Outbox };
        public static TrackedObjectKey Reassembly = new TrackedObjectKey() { ObjectType = TrackedObjectType.Reassembly };
        public static TrackedObjectKey Sessions = new TrackedObjectKey() { ObjectType = TrackedObjectType.Sessions };
        public static TrackedObjectKey Timers = new TrackedObjectKey() { ObjectType = TrackedObjectType.Timers };
        public static TrackedObjectKey Prefetch = new TrackedObjectKey() { ObjectType = TrackedObjectType.Prefetch };
        public static TrackedObjectKey Queries = new TrackedObjectKey() { ObjectType = TrackedObjectType.Queries };

        // convenient constructors for non-singletons

        public static TrackedObjectKey History(string id) => new TrackedObjectKey() 
            { 
                ObjectType = TrackedObjectType.History,
                InstanceId = id,
            };
        public static TrackedObjectKey Instance(string id) => new TrackedObjectKey() 
            {
                ObjectType = TrackedObjectType.Instance,
                InstanceId = id,
            };

        public static TrackedObject Factory(TrackedObjectKey key) => key.ObjectType switch
            {
                TrackedObjectType.Activities => new ActivitiesState(),
                TrackedObjectType.Dedup => new DedupState(),
                TrackedObjectType.Outbox => new OutboxState(),
                TrackedObjectType.Reassembly => new ReassemblyState(),
                TrackedObjectType.Sessions => new SessionsState(),
                TrackedObjectType.Timers => new TimersState(),
                TrackedObjectType.Prefetch => new PrefetchState(),
                TrackedObjectType.Queries => new QueriesState(),
                TrackedObjectType.History => new HistoryState() { InstanceId = key.InstanceId },
                TrackedObjectType.Instance => new InstanceState() { InstanceId = key.InstanceId },
                _ => throw new ArgumentException("invalid key", nameof(key)),
            };

        public static IEnumerable<TrackedObjectKey> GetSingletons() 
            => Enum.GetValues(typeof(TrackedObjectType)).Cast<TrackedObjectType>().Where(t => IsSingletonType(t)).Select(t => new TrackedObjectKey() { ObjectType = t });

        public override string ToString() 
            => this.InstanceId == null ? this.ObjectType.ToString() : $"{this.ObjectType}-{this.InstanceId}";

        public void Deserialize(BinaryReader reader)
        {
            this.ObjectType = (TrackedObjectType) reader.ReadByte();
            if (!IsSingletonType(this.ObjectType))
            {
                this.InstanceId = reader.ReadString();
            }
        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write((byte) this.ObjectType);
            if (!IsSingletonType(this.ObjectType))
            {
                writer.Write(this.InstanceId);
            }
        }
    }
}
