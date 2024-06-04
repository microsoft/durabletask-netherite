// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.AzureFunctions.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Netherite.AzureFunctions.Tests.Logging;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;
    using Xunit;
    using Xunit.Abstractions;

    public class IntegrationTestBase : IAsyncLifetime
    {
        readonly TestLogProvider logProvider;
        readonly TestFunctionTypeLocator typeLocator;
        readonly TestSettingsResolver settingsResolver;

        readonly IHost functionsHost;

        public IntegrationTestBase(ITestOutputHelper output)
        {
            this.logProvider = new TestLogProvider(output);
            this.typeLocator = new TestFunctionTypeLocator();
            this.settingsResolver = new TestSettingsResolver();

            this.settingsResolver.AddSetting("Storage", Environment.GetEnvironmentVariable(Netherite.Tests.TestConstants.StorageConnectionName));

            this.functionsHost = new HostBuilder()
                .ConfigureLogging(
                    loggingBuilder =>
                    {
                        loggingBuilder.AddProvider(this.logProvider);
                        loggingBuilder.SetMinimumLevel(LogLevel.Trace);
                    })
                .ConfigureWebJobs(
                    webJobsBuilder =>
                    {
                        webJobsBuilder.AddDurableTask(options =>
                        {
                            options.StorageProvider["type"] = NetheriteProviderFactory.ProviderName;

                            options.StorageProvider[nameof(NetheriteOrchestrationServiceSettings.PartitionCount)] = "6";

                            options.StorageProvider[nameof(NetheriteOrchestrationServiceSettings.LogLevelLimit)] = LogLevel.Trace.ToString();
                            options.StorageProvider[nameof(NetheriteOrchestrationServiceSettings.StorageLogLevelLimit)] = LogLevel.Trace.ToString();
                            options.StorageProvider[nameof(NetheriteOrchestrationServiceSettings.TransportLogLevelLimit)] = LogLevel.Trace.ToString();
                            options.StorageProvider[nameof(NetheriteOrchestrationServiceSettings.EventLogLevelLimit)] = LogLevel.Trace.ToString();
                            options.StorageProvider[nameof(NetheriteOrchestrationServiceSettings.WorkItemLogLevelLimit)] = LogLevel.Trace.ToString();

                            options.HubName = $"testhub{Guid.NewGuid():N}";
                        });
                    })
                .ConfigureServices(
                    services =>
                    {
                        services.AddSingleton<INameResolver>(this.settingsResolver);
#pragma warning disable CS0618 // Type or member is obsolete
                        services.AddSingleton<IConnectionStringResolver>(this.settingsResolver);
#pragma warning restore CS0618 // Type or member is obsolete
                        services.AddSingleton<ITypeLocator>(this.typeLocator);
                        services.AddSingleton<IDurabilityProviderFactory, NetheriteProviderFactory>();
                    })
                .Build();

            this.AddFunctions(typeof(ClientFunctions));
        }

        async Task IAsyncLifetime.InitializeAsync()
        {
           await this.functionsHost.StartAsync();
        }

        async Task IAsyncLifetime.DisposeAsync() 
        {
            try
            {
                await this.functionsHost.StopAsync();
            }
            catch(OperationCanceledException)
            {

            }
        }

        protected void AddFunctions(Type functionType) => this.typeLocator.AddFunctionType(functionType);

        protected Task CallFunctionAsync(string functionName, string parameterName, object argument)
        {
            return this.CallFunctionAsync(
                functionName,
                new Dictionary<string, object>()
                {
                    { parameterName, argument },
                });
        }

        protected Task CallFunctionAsync(string name, IDictionary<string, object> args = null)
        {
            IJobHost jobHost = this.functionsHost.Services.GetService<IJobHost>();
            return jobHost.CallAsync(name, args);
        }

        protected async Task<DurableOrchestrationStatus> RunOrchestrationAsync(string name)
        {
            IDurableClient client = await this.GetDurableClientAsync();
            string instanceId = await client.StartNewAsync(name);

            TimeSpan timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(10);
            DurableOrchestrationStatus status = await client.WaitForCompletionAsync(instanceId, timeout);
            Assert.NotNull(status);
            return status;
        }

        protected async Task<List<DurableOrchestrationStatus>> GetInstancesAsync(OrchestrationStatusQueryCondition condition)
        {
            var results = new List<DurableOrchestrationStatus>();
            IDurableClient client = await this.GetDurableClientAsync();
            do
            {
                var response = await client.ListInstancesAsync(condition, CancellationToken.None);
                results.AddRange(response.DurableOrchestrationState);
                condition.ContinuationToken = response.ContinuationToken;
            } 
            while (condition.ContinuationToken != null);
            return results;
        }

        protected async Task<PurgeHistoryResult> PurgeAllAsync()
        {
            IDurableClient client = await this.GetDurableClientAsync();
            return await client.PurgeInstanceHistoryAsync(default, default, null);
        }

        protected async Task<IDurableClient> GetDurableClientAsync()
        {
            var clientRef = new IDurableClient[1];
            await this.CallFunctionAsync(nameof(ClientFunctions.GetDurableClient), "clientRef", clientRef);
            IDurableClient client = clientRef[0];
            Assert.NotNull(client);
            return client;
        }

        protected IEnumerable<string> GetExtensionLogs()
        {
            return this.GetLogs("Host.Triggers.DurableTask");
        }

        protected IEnumerable<string> GetFunctionLogs(string functionName)
        {
            return this.GetLogs($"Function.{functionName}.User");
        }

        protected IEnumerable<string> GetProviderLogs()
        {
            return this.GetLogs($"DurableTask.Netherite");
        }

        protected IEnumerable<string> GetLogs(string category)
        {
            bool loggerExists = this.logProvider.TryGetLogs(category, out IEnumerable<LogEntry> logs);
            Assert.True(loggerExists, $"No logger was found for '{category}'.");

            return logs.Select(entry => entry.Message).ToArray();
        }

        class TestFunctionTypeLocator : ITypeLocator
        {
            readonly List<Type> functionTypes = new List<Type>();

            public void AddFunctionType(Type functionType) => this.functionTypes.Add(functionType);

            IReadOnlyList<Type> ITypeLocator.GetTypes() => this.functionTypes.AsReadOnly();
        }

#pragma warning disable CS0618 // Type or member is obsolete
       class TestSettingsResolver : INameResolver, IConnectionStringResolver
#pragma warning restore CS0618 // Type or member is obsolete
        {
            readonly Dictionary<string, string> testSettings =
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            public void AddSetting(string name, string value) => this.testSettings.Add(name, value);

            string INameResolver.Resolve(string name) => this.Resolve(name);

            string IConnectionStringResolver.Resolve(string connectionStringName) => this.Resolve(connectionStringName);

            string Resolve(string name)
            {
                if (string.IsNullOrEmpty(name))
                {
                    return null;
                }

                if (this.testSettings.TryGetValue(name, out string value))
                {
                    return value;
                }

                return Environment.GetEnvironmentVariable(name);
            }
        }

        static class ClientFunctions
        {
            [NoAutomaticTrigger]
            public static void GetDurableClient(
                [DurableClient] IDurableClient client,
                IDurableClient[] clientRef)
            {
                clientRef[0] = client;
            }
        }
    }
}
