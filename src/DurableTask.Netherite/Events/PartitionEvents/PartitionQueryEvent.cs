// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;

    [DataContract]
    abstract class PartitionQueryEvent : PartitionEvent
    {
        public abstract InstanceQuery InstanceQuery { get; }
   
        /// <summary>
        /// The continuation for the query operation.
        /// </summary>
        /// <param name="result">The tracked objects returned by this query</param>
        /// <param name="exceptionTask">A task that throws an exception if the enumeration fails</param>
        /// <param name="partition">The partition</param>
        public abstract Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> result, Task exceptionTask, Partition partition);
    }
}
