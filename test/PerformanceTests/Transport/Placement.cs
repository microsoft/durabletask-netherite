namespace PerformanceTests.Transport
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Newtonsoft.Json;

    public class Placement  
    {
        public string[] Hosts { get; set; }

        public Dictionary<Guid, string> Clients { get; set; } = new Dictionary<Guid, string>();

        public string PartitionHost(int partitionId) 
            => this.Hosts[partitionId % this.Hosts.Length];

        public string LoadMonitorHost()
             => this.Hosts[0];

        public string ClientHost(Guid clientId)
             => this.Clients[clientId];
    }
}
