// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
        /// <param name="partition">The partition</param>
        public abstract Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> result, Partition partition);
    }
}
