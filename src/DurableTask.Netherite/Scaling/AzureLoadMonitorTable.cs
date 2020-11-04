// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite.Scaling
{
    using Microsoft.Azure.Cosmos.Table;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    class AzureLoadMonitorTable : ILoadMonitorService
    {
        readonly CloudTable table;
        readonly string taskHubName;

        public AzureLoadMonitorTable(string connectionString, string tableName, string taskHubName)
        {
            var account = CloudStorageAccount.Parse(connectionString);
            this.table = account.CreateCloudTableClient().GetTableReference(tableName);
            this.taskHubName = taskHubName;
        }

        public TimeSpan PublishInterval => TimeSpan.FromSeconds(10);

        public async Task DeleteIfExistsAsync(CancellationToken cancellationToken)
        {
            if (! await this.table.ExistsAsync().ConfigureAwait(false))
            {
                return;
            }

            var query = new TableQuery<PartitionInfoEntity>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, this.taskHubName));
            TableContinuationToken continuationToken = null;
            do
            {
                var batch = await this.table.ExecuteQuerySegmentedAsync<PartitionInfoEntity>(query, continuationToken, null, null, cancellationToken).ConfigureAwait(false);

                if (batch.Count() > 0)
                {
                    // delete all entities in this batch. Max partition number is 32 so it always fits.
                    TableBatchOperation tableBatch = new TableBatchOperation();

                    foreach (var e in batch)
                    {
                        tableBatch.Add(TableOperation.Delete(e));
                    }

                    await this.table.ExecuteBatchAsync(tableBatch).ConfigureAwait(false);
                }
            }
            while (continuationToken != null);
        }

        public async Task CreateIfNotExistsAsync(CancellationToken cancellationToken)
        {
            if (await this.table.ExistsAsync().ConfigureAwait(false))
            {
                return;
            }

            await this.table.CreateAsync().ConfigureAwait(false);
        }

        public Task PublishAsync(Dictionary<uint, PartitionLoadInfo> info, CancellationToken cancellationToken)
        {
            TableBatchOperation tableBatch = new TableBatchOperation();
            foreach(var kvp in info)
            {
                tableBatch.Add(TableOperation.InsertOrReplace(new PartitionInfoEntity(this.taskHubName, kvp.Key, kvp.Value)));
            }
            return this.table.ExecuteBatchAsync(tableBatch, null, null, cancellationToken);
        }

        public async Task<Dictionary<uint, PartitionLoadInfo>> QueryAsync(CancellationToken cancellationToken)
        {
            var query = new TableQuery<PartitionInfoEntity>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, this.taskHubName));
            TableContinuationToken continuationToken = null;
            Dictionary<uint, PartitionLoadInfo> result = new Dictionary<uint, PartitionLoadInfo>();
            do
            {
                var batch = await this.table.ExecuteQuerySegmentedAsync<PartitionInfoEntity>(query, continuationToken, null, null, cancellationToken).ConfigureAwait(false);
                foreach (var e in batch)
                {
                    result.Add(e.PartitionId, new PartitionLoadInfo()
                    {
                        WorkItems = e.WorkItems,
                        Activities = e.Activities,
                        Timers = e.Timers,
                        Requests = e.Requests,
                        Wakeup = e.NextTimer,
                        Outbox = e.Outbox,
                        InputQueuePosition = e.InputQueuePosition,
                        CommitLogPosition = e.CommitLogPosition,
                        ActivityLatencyMs = e.ActivityLatencyMs,
                        WorkItemLatencyMs = e.WorkItemLatencyMs,
                        WorkerId = e.WorkerId,
                        LatencyTrend = e.LatencyTrend,
                        MissRate = e.MissRate,
                    });
                }
            }
            while (continuationToken != null);
            return result;
        }

        public class PartitionInfoEntity : TableEntity
        {
            public int WorkItems { get; set; }
            public int Activities { get; set; }
            public int Timers { get; set; }
            public int Requests { get; set; }
            public int Outbox { get; set; }
            public DateTime? NextTimer { get; set; }
            public long InputQueuePosition { get; set; }
            public long CommitLogPosition { get; set; }
            public long ActivityLatencyMs { get; set; }
            public long WorkItemLatencyMs { get; set; }
            public string WorkerId { get; set; }
            public string LatencyTrend { get; set; }
            public double MissRate { get; set; }

            public PartitionInfoEntity()
            {
            }

            // constructor for creating a deletion query; only the partition and row keys matter
            public PartitionInfoEntity(string taskHubName, uint partitionId)
                 : base(taskHubName, partitionId.ToString("D2"))
            {
                this.ETag = "*"; // no conditions when deleting
            }

            // constructor for updating load information
            public PartitionInfoEntity(string taskHubName, uint partitionId, PartitionLoadInfo info)
                : base(taskHubName, partitionId.ToString("D2"))
            {
                this.WorkItems = info.WorkItems;
                this.Activities = info.Activities;
                this.Timers = info.Timers;
                this.Requests = info.Requests;
                this.NextTimer = info.Wakeup;
                this.Outbox = info.Outbox;
                this.InputQueuePosition = info.InputQueuePosition;
                this.CommitLogPosition = info.CommitLogPosition;
                this.ActivityLatencyMs = info.ActivityLatencyMs;
                this.WorkItemLatencyMs = info.WorkItemLatencyMs;
                this.WorkerId = info.WorkerId;
                this.LatencyTrend = info.LatencyTrend;
                this.MissRate = info.MissRate;

                this.ETag = "*"; // no conditions when inserting, replace existing
            }

            public string TaskHubName => this.PartitionKey;
            public uint PartitionId => uint.Parse(this.RowKey);
        }
    }
}
