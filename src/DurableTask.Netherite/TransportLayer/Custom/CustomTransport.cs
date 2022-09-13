namespace DurableTask.Netherite.CustomTransport
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Netherite.Abstractions;
    using Microsoft.Azure.Documents.SystemFunctions;
    using Microsoft.Extensions.DependencyInjection;

    /// <summary>
    /// A base class for implementing a custom transport layer.
    /// </summary>
    public abstract class CustomTransport : ITransportLayer
    {
        readonly NetheriteOrchestrationService orchestrationService;
        readonly ConcurrentDictionary<int, PartitionInfo> partitions;
        LoadMonitorInfo loadMonitorInfo;
        ClientInfo clientInfo;

        public TaskhubParameters Parameters { get; private set; }
        public Guid ClientId => this.clientInfo.ClientId;

        internal TransportAbstraction.IHost Host => this.orchestrationService;

        // barrier for completing local startup of orchestration service
        readonly TaskCompletionSource<CustomTransport> startupCompleted = new TaskCompletionSource<CustomTransport>();
        public Task<CustomTransport> WhenOrchestrationServiceStarted => this.startupCompleted.Task;


        public CustomTransport(NetheriteOrchestrationService orchestrationService)
        {
            this.orchestrationService = orchestrationService;
            this.partitions = new ConcurrentDictionary<int, PartitionInfo>();
        }

        public abstract Task SendToPartitionAsync(int i, Stream content);
        public abstract Task SendToLoadMonitorAsync(byte[] content);
        public abstract Task SendToClientAsync(Guid clientId, byte[] content);

        public async Task DeliverToPartition(int partitionId, Stream stream)
        {
            if (!this.partitions.TryGetValue(partitionId, out PartitionInfo partitionInfo))
            {
                throw new InvalidOperationException("partition does not exist on this host");
            }
            else
            {
                await partitionInfo.DeliverAsync(stream);
            }
        }

        public Task DeliverToLoadMonitor(Stream stream)
        {
            if (this.loadMonitorInfo == null)
            {
                throw new InvalidOperationException("loadmonitor does not exist on this host");
            }
            else
            {
                this.loadMonitorInfo.Deliver(stream);
                return Task.CompletedTask;
            }
        }

        public Task DeliverToClient(Guid clientId, Stream stream)
        {
            if (this.clientInfo.ClientId != clientId)
            {
                throw new InvalidOperationException("client does not exist on this host");
            }
            else
            {
                this.clientInfo.Deliver(stream);
                return Task.CompletedTask;
            }
        }

        public async Task StartPartitionAsync(int partitionId)
        {
            await this.WhenOrchestrationServiceStarted;

            var partitionInfo = this.partitions.AddOrUpdate(
                    partitionId,
                    (partitionId) => new PartitionInfo((uint)partitionId, this),
                    (partitionId, partitionInfo) => throw new InvalidOperationException("partition already exists on this host"));

            await partitionInfo.StartupTask;
        }

        public async Task StopPartitionAsync(int partitionId)
        {
            if (this.partitions.TryRemove(partitionId, out var partitionInfo))
            {
                await partitionInfo.StopAsync();
            }
            else
            {
                throw new InvalidOperationException("partition does not exist on this host");
            }
        }

        public async Task StartLoadMonitorAsync()
        {
            await this.WhenOrchestrationServiceStarted;

            if (this.loadMonitorInfo != null)
            {
                throw new InvalidOperationException("loadmonitor already started on this host");
            }
            else
            {
                this.loadMonitorInfo = new LoadMonitorInfo(this);
            }
        }

        public async Task StopLoadMonitorAsync()
        {
            if (this.loadMonitorInfo == null)
            {
                throw new InvalidOperationException("loadmonitor does not exist on this host");
            }
            else
            {
                await this.loadMonitorInfo.StopAsync();
                this.loadMonitorInfo = null;
            }
        }

        #region ITransportLayer

        async Task<TaskhubParameters> ITransportLayer.StartAsync()
        {
            this.Parameters = await this.orchestrationService.StorageLayer.TryLoadTaskhubAsync(throwIfNotFound: true);
            return this.Parameters;
        }

        Task ITransportLayer.StartClientAsync()
        {
            this.clientInfo = new ClientInfo(Guid.NewGuid(), this);
            this.startupCompleted.TrySetResult(this);
            return Task.CompletedTask;
        }

        Task ITransportLayer.StartWorkersAsync()
        {
            return Task.CompletedTask;
        }

        async Task ITransportLayer.StopAsync()
        {
            await Task.WhenAll(
              this.clientInfo.StopAsync(),
              this.loadMonitorInfo.StopAsync(),
              Task.WhenAll(this.partitions.Select(kvp => kvp.Value.StopAsync()).ToList()));
        }

        #endregion
    }
}
