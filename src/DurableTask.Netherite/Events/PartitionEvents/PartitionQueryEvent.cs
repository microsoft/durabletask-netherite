// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;

    [DataContract]
    abstract class PartitionQueryEvent : PartitionEvent
    {
        /// <summary>
        /// The query details
        /// </summary>
        public abstract InstanceQuery InstanceQuery { get; }

        /// <summary>
        /// Optionally, a timeout for the query
        /// </summary>
        public abstract DateTime? TimeoutUtc { get; }
   
        /// <summary>
        /// The continuation token for the query
        /// </summary>
        public abstract string ContinuationToken { get; }

        /// <summary>
        /// The suggested page size returned. Actual size may be more, or less, than the suggested size.
        /// </summary>
        public abstract int PageSize { get; }

        /// <summary>
        /// The continuation for the query operation.
        /// </summary>
        /// <param name="result">The tracked objects returned by this query</param>
        /// <param name="exceptionTask">A task that throws an exception if the enumeration fails</param>
        /// <param name="partition">The partition</param>
        /// <param name="attempt">The timestamp for this query attempt</param>
        public abstract Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> result, Partition partition, DateTime attempt);
    }
}
