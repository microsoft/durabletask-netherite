namespace PerformanceTests.Transport
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    class SharedHttpClient
    {
        readonly Placement placement;
        readonly HttpClient client;

        public SharedHttpClient(Placement placement)
            : base()
        {
            this.placement = placement;
            this.client = new HttpClient();
        }

        public async Task SendToClientAsync(Guid clientId, Stream content)
        {
            string hostUri = this.placement.ClientHost(clientId);
            var response = await this.client.PostAsync($"{hostUri}/triggertransport/client/{clientId}", new StreamContent(content));
            response.EnsureSuccessStatusCode();
        }

        public async Task SendToLoadMonitorAsync(Stream content)
        {
            string hostUri = this.placement.LoadMonitorHost();
            var response = await this.client.PostAsync($"{hostUri}/triggertransport/loadmonitor", new StreamContent(content));
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
            string content = await response.Content.ReadAsStringAsync();
            JObject responseJson = JsonConvert.DeserializeObject<JObject>(content);
            return Guid.Parse((string) responseJson.GetValue("clientId"));
        }

        public async Task StartLocalAsync(string[] hosts, int index)
        {
            string hostUri = this.placement.PartitionHost(index);
            string content = JsonConvert.SerializeObject(hosts);
            var response = await this.client.PostAsync($"{hostUri}/triggertransport/startlocal/{index}", new StringContent(content));
            response.EnsureSuccessStatusCode();
        }

        public async Task<string> Test(string hostUri)
        {
            try
            {
                var response = await this.client.GetAsync($"{hostUri}/ping");
                return $"{response.StatusCode} {await response.Content.ReadAsStringAsync()}";
            }
            catch(Exception e)
            {
                return $"exception: {e}";
            }
        }
    }
}
