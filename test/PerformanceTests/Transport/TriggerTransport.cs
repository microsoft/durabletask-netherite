namespace PerformanceTests.Transport
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Netherite;
    using DurableTask.Netherite.CustomTransport;
    using Microsoft.Azure.Documents;

    public class TriggerTransport : CustomTransport
    {
        readonly SharedClient client;
        readonly Placement placement;

        public TriggerTransport(NetheriteOrchestrationService service)
            : base(service)
        {
            this.placement = new Placement();
            this.client = new SharedClient(this.placement);
        }

        // barrier for completing local startup of partitions and load monitor
        readonly TaskCompletionSource<bool> localStartComplete = new TaskCompletionSource<bool>();
        public Task WhenLocallyStarted => this.localStartComplete.Task;


        public override Task SendToClientAsync(Guid clientId, Stream content)
        {
            if (this.DeliverToLocalClient(clientId, content))
            {
                return Task.CompletedTask;
            }
            else
            {
                return this.client.SendToClientAsync(clientId, content);
            }
        }

        public override Task SendToLoadMonitorAsync(Stream content)
        {
            if (this.DeliverToLocalLoadMonitor(content))
            {
                return Task.CompletedTask;
            }
            else
            {
                return this.client.SendToLoadMonitorAsync(content);
            }
        }

        public override async Task SendToPartitionAsync(int i, Stream content)
        {
            if (!await this.DeliverToLocalPartitionAsync(i, content))
            {
                await this.client.SendToPartitionAsync(i, content);
            }
        }

        public async Task StartAllAsync(string[] hosts)
        {
            this.placement.SetHosts(hosts);
            {
                var tasks = new List<Task<Guid>>();
                for (int i = 0; i < hosts.Length; i++)
                {
                    tasks.Add(this.client.GetClientIdAsync(i));
                }
                await Task.WhenAll(tasks);
                for (int i = 0; i < hosts.Length; i++)
                {
                    this.placement.SetClientLocation(tasks[i].Result, i);
                }
            }
            {
                var tasks = new List<Task>();
                for (int i = 0; i < hosts.Length; i++)
                {
                    tasks.Add(this.client.StartLocalAsync(hosts, i));
                }
                await Task.WhenAll(tasks);
            }
        }

        public async Task StartLocalAsync(string[] hosts, int index)
        {
            this.placement.SetHosts(hosts);
            var tasks = new List<Task>();    
            for (int partitionId = 0; partitionId < this.Parameters.PartitionCount; partitionId++)
            {
                if (this.placement.PartitionHost(partitionId) == hosts[index])
                {
                    tasks.Add(this.StartPartitionAsync(partitionId));
                }
            }
            if (this.placement.LoadMonitorHost() == hosts[index])
            {
                tasks.Add(this.StartLoadMonitorAsync());
            }
            await Task.WhenAll(tasks);
            this.localStartComplete.TrySetResult(true);
        }
    }
}
