// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Scaling
{
    using Azure;
    using Azure.Data.Tables;
    using Microsoft.Extensions.Azure;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    class AzureTableLoadPublisher : ILoadPublisherService
    {
        readonly TableClient table;
        readonly string taskHubName;

        public AzureTableLoadPublisher(string connectionString, string tableName, string taskHubName)
        {
            this.table = new TableClient(connectionString, tableName); 
            this.taskHubName = taskHubName;
        }

        public TimeSpan PublishInterval => TimeSpan.FromSeconds(10);

        public async Task DeleteIfExistsAsync(CancellationToken cancellationToken)
        {
            try
            {
                var tableBatch = new List<TableTransactionAction>();
                await foreach (var e in this.table.QueryAsync<PartitionInfoEntity>(x => x.PartitionKey == this.taskhubName, cancellationToken: cancellationToken)
                {
                    tableBatch.Add(new TableTransactionAction(TableTransactionActionType.Delete, e));
                }
                await this.table.SubmitTransactionAsync(tableBatch, cancellationToken);
            }
            catch(Exception) // table may not exist
            {

            }
        }

        public Task CreateIfNotExistsAsync(CancellationToken cancellationToken)
        {
            return this.table.CreateIfNotExistsAsync(cancellationToken);
        }

        public Task PublishAsync(Dictionary<uint, PartitionLoadInfo> info, CancellationToken cancellationToken)
        {
            var tableBatch = new List<TableTransactionAction>();
            foreach(var kvp in info)
            {
                tableBatch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, new PartitionInfoEntity(this.taskHubName, kvp.Key, kvp.Value)));
            }
            return this.table.SubmitTransactionAsync(tableBatch, cancellationToken);
        }

        public async Task<Dictionary<uint, PartitionLoadInfo>> QueryAsync(CancellationToken cancellationToken)
        {
            Dictionary<uint, PartitionLoadInfo> result = new Dictionary<uint, PartitionLoadInfo>();
            await foreach (var e in this.table.QueryAsync<PartitionInfoEntity>(x => x.PartitionKey == this.taskhubName, cancellationToken: cancellationToken)
            {
                int.TryParse(e.CachePct, out int cachePct);
                double.TryParse(e.MissRate, out double missRatePct);
                result.Add(uint.Parse(e.RowKey), new PartitionLoadInfo()
                {
                    WorkItems = e.WorkItems,
                    Activities = e.Activities,
                    Timers = e.Timers,
                    Requests = e.Requests,
                    Wakeup = e.NextTimer,
                    Outbox = e.Outbox,
                    Instances = e.Instances,
                    InputQueuePosition = e.InputQueuePosition,
                    CommitLogPosition = e.CommitLogPosition,
                    WorkerId = e.WorkerId,
                    LatencyTrend = e.LatencyTrend,
                    MissRate = missRatePct / 100,
                    CachePct = cachePct,
                    CacheMB = e.CacheMB,
                });
            }
            return result;
        }

        public class PartitionInfoEntity : ITableEntity
        {
            public string PartitionKey { get; set; } // TaskHub name
            public string RowKey { get; set; } // partitionId
            public ETag ETag { get; set; }
            public DateTimeOffset? Timestamp { get; set; }

            public int WorkItems { get; set; }
            public int Activities { get; set; }
            public int Timers { get; set; }
            public int Requests { get; set; }
            public int Outbox { get; set; }
            public long Instances { get; set; }
            public DateTime? NextTimer { get; set; }
            public long InputQueuePosition { get; set; }
            public long CommitLogPosition { get; set; }
            public string WorkerId { get; set; }
            public string LatencyTrend { get; set; }
            public string MissRate { get; set; }
            public string CachePct { get; set; }
            public double CacheMB { get; set; }

            public PartitionInfoEntity()
            {
            }

            public PartitionInfoEntity(string taskHubName, uint partitionId)
            {
                this.PartitionKey = taskHubName;
                this.RowKey = partitionId.ToString("D2");
                this.ETag = ETag.All; // no conditions
            }

            public PartitionInfoEntity(string taskHubName, uint partitionId, PartitionLoadInfo info)
                : this(taskHubName, partitionId)
            {
                this.WorkItems = info.WorkItems;
                this.Activities = info.Activities;
                this.Timers = info.Timers;
                this.Requests = info.Requests;
                this.NextTimer = info.Wakeup;
                this.Outbox = info.Outbox;
                this.Instances = info.Instances;
                this.InputQueuePosition = info.InputQueuePosition;
                this.CommitLogPosition = info.CommitLogPosition;
                this.WorkerId = info.WorkerId;
                this.LatencyTrend = info.LatencyTrend;
                this.MissRate = $"{info.MissRate*100:f2}%";
                this.CachePct = $"{info.CachePct}%";
                this.CacheMB = info.CacheMB;
            }
        }
    }
}
