// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tracing
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Microsoft.Spatial;

    public class RempFormat
    {
        // a ReMP trace is a stream of entries  
        // of the form  (WorkerHeader WorkItem*)*

        // the worker header identifies the worker that issued all the worker items that follow it (until the next header)
        // the work item represents an atomic step
        // - for client work items (external inputs), the latency is zero and there are no consumed messages
        // - for stateless work items, there is only one consumed message, and no instance State information

        // importantly, the destination of a message is NOT included. Rather, it can be deduced from where it is consumed.
        // This allows us to abstract all the load balancing (e.g. hashing of instance ids into partitions, placing of partitions on workers, global balancing of stateless work items)

        public interface IListener 
        {
            void WorkerHeader(
                string workerId,                             // a unique identifier for this worker.
                IEnumerable<WorkitemGroup> groups);          // A list of work item groups.

            void WorkItem(
                long timeStamp,                              // the time at which this work item was completed.
                string workItemId,                           // a unique id for this work item. Used for debugging.
                int group,                                   // the group this work item belongs to. Is an index into the list of groups in the header.
                double latencyMs,                            // the measured execution latency of the user code in milliseconds.
                IEnumerable<NamedPayload> consumedMessages,  // the messages consumed by this work item.
                IEnumerable<NamedPayload> producedMessages,  // the messages produced by this work item.
                InstanceState? instanceState);               // state information for stateful work items.
        }

        public struct WorkitemGroup // represents categories of work items that the application distinguishes between
        {
            public string Name;                // A name for the group.
            public int? DegreeOfParallelism;   // a per-worker limit on the number of work items in this group that may run in parallel
        }

        public struct NamedPayload  // represents abstracted immutable data payloads moving through the system
        {
            public string Id;       // a unique identifier for this piece of data
            public long NumBytes;   // the number of bytes needed
        }

        public struct InstanceState // represents information about the instance state
        {
            public string InstanceId;          // a unique identifier for this instance.
            public long? Updated;              // size of the updated state (0 means deleted), or null if state was not modified
        }
    }
}
