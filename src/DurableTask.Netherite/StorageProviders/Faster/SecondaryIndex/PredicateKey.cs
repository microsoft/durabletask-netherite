// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#pragma warning disable IDE0008 // Use explicit type

namespace DurableTask.Netherite.Faster
{
    using System;
    using DurableTask.Core;
    using FASTER.core;

    struct PredicateKey
    {
        internal static TimeSpan DateBinInterval = TimeSpan.FromMinutes(1);
        internal static DateTime BaseDate = new(2020, 1, 1);
        internal const int InstanceIdPrefixLen7 = 7;
        internal const int InstanceIdPrefixLen4 = 4;

        enum PredicateColumn { Default = 0, RuntimeStatus = 101, CreatedTime, InstanceIdPrefix7, InstanceIdPrefix4 }

        // Enums are not blittable
        readonly int column;
        readonly int value;

        internal PredicateKey(OrchestrationStatus status)
        {
            this.column = (int)PredicateColumn.RuntimeStatus;
            this.value = (int)status;
        }

        internal PredicateKey(DateTime dt)
        {
            this.column = (int)PredicateColumn.CreatedTime;

            // Make bins of one minute, starting from the beginning of 2020.
            var ts = TimeSpan.FromTicks((dt - BaseDate).Ticks);
            this.value = (int)Math.Floor(ts.TotalMinutes);
        }

        internal PredicateKey(string instanceId, int prefixLength)    // TODO change this to pass a list of prefixFunc<string, string> and make a Predicate for each? E.g. parse "@{entityName.ToLowerInvariant()}@" or "@"
        {
            this.column = prefixLength == InstanceIdPrefixLen7 ? (int)PredicateColumn.InstanceIdPrefix7 : (int)PredicateColumn.InstanceIdPrefix4;
            this.value = GetInvariantHashCode(MakeInstanceIdPrefix(instanceId, prefixLength));
        }

        static string MakeInstanceIdPrefix(string instanceId, int prefixLength) 
            => instanceId.Length > prefixLength
                ? instanceId.Substring(0, Math.Min(instanceId.Length, prefixLength))
                : instanceId;

        static int GetInvariantHashCode(string item)
        {
            // NetCore randomizes string.GetHashCode() per-appdomain, to prevent hash flooding.
            // Therefore it's important to verify for each call site that this isn't a concern.
            // This is a non-unsafe/unchecked version of (internal) string.GetLegacyNonRandomizedHashCode().
            unchecked
            {
                int hash1 = (5381 << 16) + 5381;
                int hash2 = hash1;

                for (var ii = 0; ii < item.Length; ii += 2)
                {
                    hash1 = ((hash1 << 5) + hash1) ^ item[ii];
                    if (ii < item.Length - 1)
                    {
                        hash2 = ((hash2 << 5) + hash2) ^ item[ii + 1];
                    }
                }
                return hash1 + (hash2 * 1566083941);
            }
        }

        OrchestrationStatus Status => (OrchestrationStatus)this.value;

        public override string ToString()
            => (PredicateColumn)this.column switch
            {
                PredicateColumn.RuntimeStatus => $"{(PredicateColumn)this.column} = {this.Status}",
                PredicateColumn.CreatedTime => $"{(PredicateColumn)this.column} = {BaseDate + TimeSpan.FromMinutes(this.value):s}",
                PredicateColumn.InstanceIdPrefix7 or PredicateColumn.InstanceIdPrefix4 => $"{(PredicateColumn)this.column} = {this.value}",
                _ => "<Unknown PredicateColumn value>"
            };

        public class Comparer : IFasterEqualityComparer<PredicateKey>
        {
            public bool Equals(ref PredicateKey k1, ref PredicateKey k2) => k1.column == k2.column && k1.value == k2.value;

            public long GetHashCode64(ref PredicateKey k) => Utility.GetHashCode(k.column) ^ Utility.GetHashCode(k.value);
        }
    }
}
