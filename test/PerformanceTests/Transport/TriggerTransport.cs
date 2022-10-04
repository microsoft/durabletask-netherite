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
        readonly SharedHttpClient httpClient;
        readonly Placement placement;

        public TriggerTransport(NetheriteOrchestrationService service)
            : base(service)
        {
            this.placement = new Placement();
            this.httpClient = new SharedHttpClient(this.placement);
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
                return this.httpClient.SendToClientAsync(clientId, content);
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
                return this.httpClient.SendToLoadMonitorAsync(content);
            }
        }

        public override async Task SendToPartitionAsync(int i, Stream content)
        {
            if (!await this.DeliverToLocalPartitionAsync(i, content))
            {
                await this.httpClient.SendToPartitionAsync(i, content);
            }
        }

        public async Task StartAllAsync(string[] hosts)
        {
            this.placement.Hosts = hosts;
            {
                var tasks = new List<Task<Guid>>();
                for (int i = 0; i < hosts.Length; i++)
                {
                    tasks.Add(this.httpClient.GetClientIdAsync(i));
                }
                await Task.WhenAll(tasks);
                for (int i = 0; i < hosts.Length; i++)
                {
                    this.placement.Clients[tasks[i].Result] = this.placement.Hosts[i];
                }
            }
            {
                var tasks = new List<Task>();
                for (int i = 0; i < hosts.Length; i++)
                {
                    tasks.Add(this.httpClient.StartLocalAsync(this.placement, i));
                }
                await Task.WhenAll(tasks);
            }
        }

        public async Task StartLocalAsync(string[] hosts, Dictionary<Guid,string> clients, int index)
        {
            this.placement.Hosts = hosts;
            this.placement.Clients = clients;

            var tasks = new List<Task>();    
            for (int partitionId = 0; partitionId < this.Parameters.PartitionCount; partitionId++)
            {
                if (this.placement.PartitionHost(partitionId) == this.placement.Hosts[index])
                {
                    tasks.Add(this.StartPartitionAsync(partitionId));
                }
            }
            if (this.placement.LoadMonitorHost() == this.placement.Hosts[index])
            {
                tasks.Add(this.StartLoadMonitorAsync());
            }
            await Task.WhenAll(tasks);
            this.localStartComplete.TrySetResult(true);
        }

        public Task<string> Test(string host)
        {
            return this.httpClient.Test(host);
        }
    }
}
