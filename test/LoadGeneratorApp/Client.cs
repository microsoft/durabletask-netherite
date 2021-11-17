// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LoadGeneratorApp
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// A static client object, to be shared by all robots on the same node
    /// </summary>
    internal class Client
    {
        private readonly Random random = new Random();
        private readonly BaseParameters parameters;

        private static readonly SemaphoreSlim asyncLock = new SemaphoreSlim(1, 1);
        private static Client client;
        private static int referenceCount;

        private ConcurrentDictionary<Guid, TaskCompletionSource<string>> continuations;

        public HttpClient HttpClient { get; private set; }

        public static async Task<Client> AcquireAsync(BaseParameters parameters, ILogger logger)
        {
            logger.LogDebug($"waiting to acquire client");
            try
            {
                await asyncLock.WaitAsync();

                if (client == null)
                {
                    logger.LogDebug($"starting client...");
                    client = new Client(parameters);
                    await client.StartAsync();
                    logger.LogDebug($"client started.");
                }

                referenceCount++;

                logger.LogDebug($"acquired client referenceCount={referenceCount}");

                return client;
            }
            finally
            {
                asyncLock.Release();
            }
        }

        public static async Task ReleaseAsync(ILogger logger)
        {
            var current = Interlocked.Decrement(ref referenceCount);

            logger.LogDebug($"releasing client referenceCount={current}");

            if (current == 0)
            {
                logger.LogDebug($"stopping client...");
                await client.StopAsync();
                client = null;
                logger.LogDebug($"client stopped.");
            }
        }

        public static void DeliverCallback(Guid reqId, string content)
        {
            client?.Callback(reqId, content);
        }

        public string BaseUrl()
        {
            string[] urls = parameters.ServiceUrls.Split(new char[] { ' ' });
            return urls[random.Next(urls.Length)];
        }

        private Client(BaseParameters parameters)
        {
            this.parameters = parameters;
            this.HttpClient = new HttpClient();
            this.HttpClient.Timeout = TimeSpan.FromSeconds(parameters.TimeoutSeconds + 20); // prefer reported timeout to http timeout
            this.continuations = new ConcurrentDictionary<Guid, TaskCompletionSource<string>>();
        }

        private Task<Client> StartAsync()
        {
            return Task.FromResult(this);
        }

        private Task StopAsync()
        {
            return Task.CompletedTask;
        }

        public async Task<StatTuple> RunOrchestrationAsync(ILogger logger, string name, string instanceId, object input, bool useReportedLatency)
        {
            string url = this.BaseUrl() + "/genericHttp";
           
            var args = new
            {
                Name = name,
                InstanceId = instanceId,
                Input = input,
                Timeout = parameters.TimeoutSeconds,
                UseReportedLatency = useReportedLatency,
            };

            string content = JsonConvert.SerializeObject(args);

            var response = await this.HttpClient.PostAsync(url, new StringContent(content)).ConfigureAwait(false);

            if (response.StatusCode == HttpStatusCode.OK)
            {
                var result = await response.Content.ReadAsStringAsync();
                var statTuple = JsonConvert.DeserializeObject<StatTuple>(result);
                return statTuple;
            }
            else if (response.StatusCode == HttpStatusCode.RequestTimeout || response.StatusCode == HttpStatusCode.Accepted)
            {
                throw new TimeoutException();
            }
            else
            {
                string errorContent = await response.Content.ReadAsStringAsync();
                throw new HttpRequestException($"{response.ReasonPhrase}: {errorContent}");
            }
        }

        public void Callback(Guid reqId, string content)
        {
            if (this.continuations.TryRemove(reqId, out var tcs))
            {
                tcs.TrySetResult(content);
            }
        }

        // matches the definition in the AWS wrapper
        public struct CallbackResponse
        {
#pragma warning disable 0649
            public DateTime StartTime;
            public DateTime EndTime;
            public double CompletionTime;
#pragma warning restore 0649
        }

        public async Task<CallbackResponse> RunRemoteRequestWithCallback(Func<Guid,string> callbackUri, string requestUri, Func<string,JObject> input)
        {
            Guid wReqId = Guid.NewGuid();

            var tcs = new TaskCompletionSource<string>();
            var c = new CancellationTokenSource();

            var timeout = TimeSpan.FromSeconds(parameters.TimeoutSeconds);
            Task timeoutTask = Timeout();
            async Task Timeout()
            {
                try
                {
                    await Task.Delay(timeout, c.Token);
                    tcs.TrySetException(new TimeoutException($"no response after {timeout}"));
                }
                catch (OperationCanceledException)
                {
                }
            }

            continuations[wReqId] = tcs;

            try
            {
                var inputObject = input(callbackUri(wReqId));
                var content = inputObject.ToString(Formatting.Indented);
                HttpResponseMessage response = await HttpClient.PostAsync(requestUri, new StringContent(content)).ConfigureAwait(false);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new HttpRequestException(response.ReasonPhrase);
                }

                string result = await tcs.Task;
                var callbackResponse = JsonConvert.DeserializeObject<CallbackResponse>(result);
                return callbackResponse;
            }
            finally
            {
                continuations.TryRemove(wReqId, out _);
                c.Cancel();
                await timeoutTask.ConfigureAwait(false);
            }
        }

        public Task<CallbackResponse> RunWrappedStepFunctionAsync(Func<Guid, string> callbackUri, string stateMachineArn, JToken stateMachineInput)
        {
            string requestUri = AwsParameters.ServiceUrl;

            JObject input(string callbackUri) {
                JObject wrapperInput = new JObject(
                    new JProperty("StateMachineInput", stateMachineInput),
                    new JProperty("StateMachineArn", stateMachineArn),
                    new JProperty("CallbackUri", callbackUri));
                return new JObject(
                    new JProperty("input", wrapperInput.ToString()),
                    new JProperty("stateMachineArn", AwsParameters.SyncWrapper_Arn));
            };

            return this.RunRemoteRequestWithCallback(callbackUri, requestUri, input);
        }
    }
}
