// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;

    /// <summary>
    /// Is used while applying an effect to a partition state, to carry
    /// information about the context, and to enumerate the objects on which the effect
    /// is being processed.
    /// </summary>
    abstract class PartitionEffectTracker : EffectTracker
    {
        readonly Partition partition;

        public PartitionEffectTracker(Partition partition) 
        {
            this.partition = partition;
            if (partition == null)
            {
                throw new ArgumentNullException(nameof(partition));
            }
        }

        public override Partition Partition => this.partition;

        public override EventTraceHelper EventTraceHelper
            => this.Partition.EventTraceHelper;

        public override EventTraceHelper EventDetailTracer
            => this.Partition.EventDetailTracer;

        protected override void HandleError(string where, string message, Exception e, bool terminatePartition, bool reportAsWarning)
            => this.Partition.ErrorHandler.HandleError(where, message, e, terminatePartition, reportAsWarning);

        public override void Assert(bool condition, string message)
            => this.Partition.Assert(condition, message);

        public override uint PartitionId
            => this.Partition.PartitionId;

        public override double CurrentTimeMs
            => this.Partition.CurrentTimeMs;
    }
}
