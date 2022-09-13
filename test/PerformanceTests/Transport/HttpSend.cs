namespace PerformanceTests.Transport
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;

    class SharedClient
    {
        readonly Placement placement;
        readonly HttpClient client;

        public SharedClient(Placement placement)
            : base()
        {
            this.placement = placement;
            this.client = new HttpClient();
        }

        public async Task SendToClientAsync(Guid clientId, byte[] content)
        {
            string hostUri = this.placement.ClientHost(clientId);
            var response = await this.client.PostAsync($"{hostUri}/triggertransport/client/{clientId}", new ByteArrayContent(content));
            response.EnsureSuccessStatusCode();
        }

        public async Task SendToLoadMonitorAsync(byte[] content)
        {
            string hostUri = this.placement.LoadMonitorHost();
            var response = await this.client.PostAsync($"{hostUri}/triggertransport/loadmonitor", new ByteArrayContent(content));
            response.EnsureSuccessStatusCode();
        }

        public async Task SendToPartitionAsync(int i, Stream content)
        {
            string hostUri = this.placement.PartitionHost(i);
            var response = await this.client.PostAsync($"{hostUri}/triggertransport/partition/{i}", new StreamContent(content));
            response.EnsureSuccessStatusCode();
        }

        public async Task<Guid> GetClientIdAsync(int hostIndex)
        {
            string hostUri = this.placement.Hosts[hostIndex];
            var response = await this.client.GetAsync($"{hostUri}/triggertransport/client");
            response.EnsureSuccessStatusCode();
            return Guid.Parse(response.Content.ToString());
        }

        public async Task StartLocalAsync(string[] hosts, int index)
        {
            string hostUri = this.placement.PartitionHost(index);
            string content = JsonConvert.SerializeObject(hosts);
            var response = await this.client.PostAsync($"{hostUri}/triggertransport/startlocal/{index}", new StringContent(content));
            response.EnsureSuccessStatusCode();
        }
    }
}
