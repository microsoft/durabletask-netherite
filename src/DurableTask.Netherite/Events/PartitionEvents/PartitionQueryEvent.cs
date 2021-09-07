// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;

    abstract class PartitionQueryEvent : PartitionEvent
    {
        public abstract InstanceQuery InstanceQuery { get; }

        public abstract int? PageSize { get; }

        public abstract string ContinuationToken { get; }

        public int PageSizeResult { get; set; }

        public string ContinuationTokenResult { get; set; }

        /// <summary>
        /// The continuation for the query operation.
        /// </summary>
        /// <param name="result">The tracked objects returned by this query</param>
        /// <param name="partition">The partition</param>
        public abstract Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> result, Partition partition);
    }
}
