// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Extensions.Logging;

    class OrchestrationWorkItem : TaskOrchestrationWorkItem
    {
        public OrchestrationMessageBatch MessageBatch { get; set; }

        public Partition Partition { get; }

        public ExecutionType Type { get; set; }

        public enum ExecutionType { Fresh, ContinueFromHistory, ContinueFromCursor };

        public List<string> NewMessagesOrigin { get; set; }

        public OrchestrationWorkItem(Partition partition, OrchestrationMessageBatch messageBatch, List<HistoryEvent> previousHistory = null)
        {
            this.Partition = partition;
            this.MessageBatch = messageBatch;
            this.InstanceId = messageBatch.InstanceId;
            this.NewMessages = messageBatch.MessagesToProcess;
            this.NewMessagesOrigin = messageBatch.MessagesToProcessOrigin;
            this.OrchestrationRuntimeState = new OrchestrationRuntimeState(previousHistory);
            this.LockedUntilUtc = DateTime.MaxValue; // this backend does not require workitem lock renewals
            this.Session = null; // we don't use the extended session API because we are caching cursors in the work item
        }

        public void SetNextMessageBatch(OrchestrationMessageBatch messageBatch)
        {
            this.MessageBatch = messageBatch;
            this.NewMessages = messageBatch.MessagesToProcess;
            this.NewMessagesOrigin = messageBatch.MessagesToProcessOrigin;
            this.OrchestrationRuntimeState.NewEvents.Clear();
        }
    }
}
