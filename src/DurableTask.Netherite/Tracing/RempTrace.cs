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

    public class RempTrace
    {
        public interface IListener 
            // processes a stream of entries  
            // of the form  (WorkerHeader WorkItem*)*
        {
            void WorkerHeader(
                string workerId,                             // a unique identifier for this worker.
                IEnumerable<WorkitemGroup> groups);          // A list of work item groups.

            void WorkItem(
                long timeStamp,                              // the time at which this work item was completed.
                string workItemId,                           // a unique id for this work item. Used for debugging.
                int group,                                   // the group this work item belongs to. Is an index into the list of groups in the header.
                double latencyMs,                            // the measured execution latency of the user code.
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
            public string Id;       // a unique identifier for this datum
            public long NumBytes;   // the number of bytes needed
        }

        public struct InstanceState // represents information about the instance state
        {
            public string InstanceId;          // a unique identifier for this instance.
            public long? Updated;              // size of the updated state, if updated (0 for deleted)
        }

        public class RempWriter : BinaryWriter, IListener
        {
            public RempWriter(Stream stream)
                : base(stream)
            {
            }

            void Assert(bool condition, string name)
            {
                if (!condition)
                {
                    throw new ArgumentException($"invalid field or parameter", name);
                }
            }

            public void WorkerHeader(string workerId, IEnumerable<WorkitemGroup> groups)
            {
                this.Write(0L);
                this.Assert(!string.IsNullOrEmpty(workerId), nameof(workerId));
                this.Write(workerId);
                foreach(var group in groups)
                {
                    this.Write(group);
                }
                this.Write(string.Empty); // terminates the group list
            }

            public void WorkItem(long timeStamp, string workItemId, int group, double latencyMs, IEnumerable<NamedPayload> consumedMessages, IEnumerable<NamedPayload> producedMessages, InstanceState? instanceState)
            {
                this.Assert(timeStamp > 100, nameof(timeStamp));
                this.Write(timeStamp);
                this.Assert(!string.IsNullOrEmpty(workItemId), nameof(workItemId));
                this.Write(workItemId);
                this.Write(group);
                this.Write(latencyMs);
                foreach(var message in consumedMessages)
                {
                    this.Write(message);
                }
                this.Write(string.Empty); // terminates the message list
                foreach (var message in producedMessages)
                {
                    this.Write(message);
                }
                this.Write(string.Empty); // terminates the message list
                this.Write(instanceState.HasValue);
                if (instanceState.HasValue)
                {
                    this.Write(instanceState.Value);
                }
                throw new NotImplementedException();
            }

            public void Write(WorkitemGroup group)
            {
                this.Assert(!string.IsNullOrEmpty(group.Name), nameof(WorkitemGroup.Name));
                this.Write(group.Name);
                this.Write(group.DegreeOfParallelism.HasValue);
                if (group.DegreeOfParallelism.HasValue)
                {
                    Debug.Assert(group.DegreeOfParallelism.Value > 0);
                    this.Write(group.DegreeOfParallelism.Value);
                }
            }

            public void Write(NamedPayload namedPayload)
            {
                this.Assert(!string.IsNullOrEmpty(namedPayload.Id), nameof(NamedPayload.Id));
                this.Write(namedPayload.Id);
                this.Assert(namedPayload.NumBytes > 0, nameof(NamedPayload.NumBytes));
                this.Write(namedPayload.NumBytes);
            }

            public void Write(InstanceState instanceState)
            {
                this.Assert(!string.IsNullOrEmpty(instanceState.InstanceId), nameof(InstanceState.InstanceId));
                this.Write(instanceState.InstanceId);
                this.Write(instanceState.Updated.HasValue);
                if (instanceState.Updated.HasValue)
                {
                    this.Write(instanceState.Updated.Value);
                }
            }
        }

        public class RempReader : BinaryReader
        {
            public RempReader(Stream stream)
                : base(stream)
            {
            }

            void Assert(bool condition, string message)
            {
                if (!condition)
                {
                    throw new FormatException($"invalid file format: {message}");
                }
            }

            public void Read(IListener listener)
            {
                long versionOrTimestamp = this.ReadInt64();
                if (versionOrTimestamp < 100)
                {
                    // we are reading a worker header
                    long version = versionOrTimestamp;
                    this.Assert(version == 0, "expected version 0");
                    string workerId = this.ReadString();
                    IEnumerable<WorkitemGroup> groups = this.ReadList(this.ReadWorkitemGroup).ToList();
                    listener.WorkerHeader(workerId, groups);
                }
                else
                {
                    // we are reading a work item
                    long timeStamp = versionOrTimestamp;
                    string workItemId = this.ReadString();
                    int group = this.ReadInt32();
                    double latencyMs = this.ReadDouble();
                    IEnumerable<NamedPayload> consumedMessages = this.ReadList(this.ReadNamedPayload).ToList();
                    IEnumerable<NamedPayload> producedMessages = this.ReadList(this.ReadNamedPayload).ToList();
                    InstanceState? instanceState = this.ReadBoolean() ? this.ReadInstanceState() : null;
                    listener.WorkItem(timeStamp, workItemId, group, latencyMs, consumedMessages, producedMessages, instanceState);
                }               
            }

            IEnumerable<T> ReadList<T>(Func<string, T> readNext)
            {
                string lookahead = this.ReadString();
                if (lookahead.Length == 0)
                {
                    // this is the termination marker
                    yield break;
                }
                else
                {
                    yield return readNext(lookahead);
                }
            }

            public WorkitemGroup ReadWorkitemGroup(string lookahead)
            {
                return new WorkitemGroup()
                {
                    Name = lookahead,
                    DegreeOfParallelism = this.ReadBoolean() ? this.ReadInt32() : null,
                };
            }

            public NamedPayload ReadNamedPayload(string lookahead)
            {
                return new NamedPayload()
                {
                    Id = lookahead,
                    NumBytes = this.ReadInt64(),
                };
            }

            public InstanceState ReadInstanceState()
            {
                return new InstanceState()
                {
                    InstanceId = this.ReadString(),
                    Updated = this.ReadBoolean() ? this.ReadInt64() : null,
                };
            }  
        }
    }
}
