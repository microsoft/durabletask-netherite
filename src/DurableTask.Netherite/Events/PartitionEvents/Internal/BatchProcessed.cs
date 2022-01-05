// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;
    using DurableTask.Core;
    using DurableTask.Core.History;

    [DataContract]
    class BatchProcessed : PartitionUpdateEvent, IRequiresPrefetch
    {
        [DataMember]
        public long SessionId { get; set; }

        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public long BatchStartPosition { get; set; }

        [DataMember]
        public int BatchLength { get; set; }

        [DataMember]
        public List<HistoryEvent> NewEvents { get; set; }

        [DataMember]
        public string CustomStatus { get; set; }

        [DataMember]
        public bool CustomStatusUpdated { get; set; }

        [DataMember]
        public bool NotExecutable { get; set; }

        [DataMember]
        public string ExecutionId { get; set; }

        [DataMember]
        public OrchestrationStatus OrchestrationStatus { get; set; }

        [DataMember]
        public List<TaskMessage> ActivityMessages { get; set; }

        [DataMember]
        public List<TaskMessage> LocalMessages { get; set; }

        [DataMember]
        public List<TaskMessage> RemoteMessages { get; set; }

        [DataMember]
        public List<TaskMessage> TimerMessages { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [DataMember]
        public PersistFirstStatus PersistFirst { get; set; }

        public enum PersistFirstStatus {  NotRequired, Required, Done };

        [DataMember]
        public int PackPartitionTaskMessages { get; set; }

        [IgnoreDataMember]
        public OrchestrationWorkItem WorkItemForReuse { get; set; }

        [IgnoreDataMember]
        public string WorkItemId => SessionsState.GetWorkItemId(this.PartitionId, this.SessionId, this.BatchStartPosition);

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(this.PersistFirst == PersistFirstStatus.Done ? this.WorkItemId + "P" : this.WorkItemId);

        [IgnoreDataMember]
        public override string TracedInstanceId => this.InstanceId;

        [IgnoreDataMember]
        public List<WaitResponseReceived> ResponsesToSend { get; set; } // used to communicate responses to ClientState

        IEnumerable<TrackedObjectKey> IRequiresPrefetch.KeysToPrefetch
        {
            get
            {
                yield return TrackedObjectKey.Instance(this.InstanceId);
                yield return TrackedObjectKey.History(this.InstanceId);
            }
        }

        public override void DetermineEffects(EffectTracker effects)
        {
            // start on the sessions object; further effects are determined from there
            effects.Add(TrackedObjectKey.Sessions);
        }

        public override void ApplyTo(TrackedObject trackedObject, EffectTracker effects)
        {
            trackedObject.Process(this, effects);
        }

        public IEnumerable<TaskMessage> LoopBackMessages()
        {
            if (this.ActivityMessages != null)
            {
                foreach (var message in this.ActivityMessages)
                {
                    yield return message;
                }
            }
            if (this.LocalMessages != null)
            {
                foreach (var message in this.LocalMessages)
                {
                    yield return message;
                }
            }
            if (this.TimerMessages != null)
            {
                foreach (var message in this.TimerMessages)
                {
                    yield return message;
                }
            }
        }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            base.ExtraTraceInformation(s);

            s.Append(' ');
            s.Append(this.OrchestrationStatus);
        }
    }
}
