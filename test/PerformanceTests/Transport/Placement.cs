namespace PerformanceTests.Transport
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;

    class Placement  
    {
        string[] hosts;
        readonly Dictionary<Guid, string> clients;

        public Placement()
        {
            this.clients = new Dictionary<Guid, string>();
        }

        public void SetHosts(string[] hosts)
        {
            this.hosts = hosts;
        }

        public void SetClientLocation(Guid clientId, int index)
        {
            this.clients[clientId] = this.hosts[index];
        }

        public IList<string> Hosts => this.hosts;
        public IEnumerable<Guid> Clients => this.clients.Keys;

        public string PartitionHost(int partitionId) 
            => this.hosts[partitionId % this.hosts.Length];
        public string LoadMonitorHost()
             => this.hosts[0];
        public string ClientHost(Guid clientId)
             => this.clients[clientId];
    }
}
