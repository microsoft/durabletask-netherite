// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tracing
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    public class RempTrace
    {
        public interface IListener 
            // processes a stream of entries  
            // of the form  (WorkerHeader WorkItem*)*
        {
            void WorkerHeader(
                int version,                                 // a version number for the format.
                string workerId,                             // a unique identifier for this worker.
                IEnumerable<WorkitemGroup> groups);          // A list of work item groups.

            void WorkItem(
                long timeStamp,
                string workItemId,                           // a unique id for this work item. Used for debugging.
                int group,                                   // the group this work item belongs to. Is an index into the list of groups in the header.
                double latencyMs,                            // the measured execution latency of the user code.
                IEnumerable<NamedPayload> consumedMessages,  // the messages consumed by this work item.
                IEnumerable<NamedPayload> producedMessages,  // the messages produced by this work item.
                InstanceState? instanceState);               // state information for stateful work items
        }

        public struct WorkitemGroup // represents categories of work items that the application distinguishes between
        {
            public string Name;                // A name for the group.
            public int? DegreeOfParallelism;   // a per-worker limit on the number of work items in this group that may run in parallel
        }

        public struct NamedPayload  // represents abstracted immutable data payloads moving through the system
        {
            public string Id;       // a unique identifier for this datum
            public long NumBytes;   // the number of bytes needed
        }

        public struct InstanceState // represents information about the instance state
        {
            public string InstanceId;          // a unique identifier for this instance.
            public long? Updated;              // size of the updated state, if updated (0 for deleted)
        }

        public class TraceWriter : IListener
        {
            void IListener.WorkerHeader(int version, string workerId, IEnumerable<WorkitemGroup> groups)
            {
                 
            }

            void IListener.WorkItem(long timeStamp, string workItemId, int group, double latencyMs, IEnumerable<NamedPayload> consumedMessages, IEnumerable<NamedPayload> producedMessages, InstanceState? instanceState)
            {
                throw new NotImplementedException();
            }
        }
    }
}
