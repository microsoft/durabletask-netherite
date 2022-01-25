// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Netherite;
    using Microsoft.Extensions.Logging;

    sealed class TestOrchestrationHost : IDisposable
    {
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly NetheriteOrchestrationService orchestrationService;
        readonly TaskHubWorker worker;
        readonly TaskHubClient client;
        readonly HashSet<Type> addedOrchestrationTypes;
        readonly HashSet<Type> addedActivityTypes;

        public TestOrchestrationHost(NetheriteOrchestrationServiceSettings settings, ILoggerFactory loggerFactory)
        {
            this.orchestrationService = new Netherite.NetheriteOrchestrationService(settings, loggerFactory);
            var orchestrationService = (IOrchestrationService)this.orchestrationService;

            if (TestConstants.DeleteStorageBeforeRunningTests)
            {
                orchestrationService.DeleteAsync().GetAwaiter().GetResult();
            }

            orchestrationService.CreateAsync(false).GetAwaiter().GetResult();

            this.settings = settings;

            this.worker = new TaskHubWorker(this.orchestrationService);
            this.client = new TaskHubClient(this.orchestrationService);
            this.addedOrchestrationTypes = new HashSet<Type>();
            this.addedActivityTypes = new HashSet<Type>();
        }

        public string TaskHub => this.settings.HubName;

        public void Dispose()
        {
            this.worker.Dispose();
        }

        public async Task StartAsync()
        {
            Trace.TraceInformation($"TestProgress: Started {this.orchestrationService}");

            await this.worker.StartAsync();
        }

        public Task StopAsync(bool quickly)
        {
            return this.worker.StopAsync(quickly);
        }

        public void AddAutoStartOrchestrator(Type type)
        {
            if (this.addedOrchestrationTypes.Add(type))
            {
                this.worker.AddTaskOrchestrations(new AutoStartOrchestrationCreator(type));
            }
        }

        void AddTypes(Type orchestrationType)
        {
            if (!this.addedOrchestrationTypes.Contains(orchestrationType))
            {
                this.addedOrchestrationTypes.Add(orchestrationType);
                this.worker.AddTaskOrchestrations(orchestrationType);
            }

            // Allow orchestration types to declare which activity types they depend on.
            // CONSIDER: Make this a supported pattern in DTFx?
            KnownTypeAttribute[] knownTypes =
                (KnownTypeAttribute[])orchestrationType.GetCustomAttributes(typeof(KnownTypeAttribute), false);

            foreach (KnownTypeAttribute referencedKnownType in knownTypes)
            {
                bool orch = referencedKnownType.Type.IsSubclassOf(typeof(TaskOrchestration));
                bool activ = referencedKnownType.Type.IsSubclassOf(typeof(TaskActivity));
                if (orch && !this.addedOrchestrationTypes.Contains(referencedKnownType.Type))
                {
                    this.worker.AddTaskOrchestrations(referencedKnownType.Type);
                    this.addedOrchestrationTypes.Add(referencedKnownType.Type);
                }

                else if (activ && !this.addedActivityTypes.Contains(referencedKnownType.Type))
                {
                    this.worker.AddTaskActivities(referencedKnownType.Type);
                    this.addedActivityTypes.Add(referencedKnownType.Type);
                }
            }
        }

        public async Task<TestOrchestrationClient> StartOrchestrationAsync(
             Type orchestrationType,
             object input,
             string instanceId = null,
             DateTime? startAt = null)
        {
            this.AddTypes(orchestrationType);
            DateTime creationTime = DateTime.UtcNow;

            TimeSpan timeout = TimeSpan.FromSeconds(50);
            timeout = TestOrchestrationClient.AdjustTimeout(timeout);
            Task timeoutTask = Task.Delay(timeout);

            Task<OrchestrationInstance>  startTask = startAt.HasValue ?
                this.client.CreateScheduledOrchestrationInstanceAsync(
                    orchestrationType,
                    instanceId,
                    input,
                    startAt.Value)
                    :
                this.client.CreateOrchestrationInstanceAsync(
                    orchestrationType,
                    instanceId,
                    input);

            await Task.WhenAny(timeoutTask, startTask);

            if (!startTask.IsCompleted)
            {
                Trace.TraceWarning( "TestProgress: Timed out {0} failed to start after {1}",
                                    orchestrationType.Name,
                                    timeout);

                throw new TimeoutException($"Orchestration {orchestrationType.Name} failed to start after {timeout}.");
            }

            var instance = await startTask;

            Trace.TraceInformation($"TestProgress: Started {orchestrationType.Name} id={instance.InstanceId}");
            return new TestOrchestrationClient(this.client, orchestrationType, instance.InstanceId, creationTime);
        }

        public async Task<(bool, TestOrchestrationClient)> StartDeduplicatedOrchestrationAsync(
            Type orchestrationType,
            object input,
            string instanceId,
            OrchestrationStatus[] stati = null)
        {
            this.AddTypes(orchestrationType);
            DateTime creationTime = DateTime.UtcNow;

            TimeSpan timeout = TimeSpan.FromSeconds(50);
            timeout = TestOrchestrationClient.AdjustTimeout(timeout);
            Task timeoutTask = Task.Delay(timeout);

            bool duplicate;
            stati ??= this.dedupeStatuses;

            try
            {
                var startTask = this.client.CreateOrchestrationInstanceAsync(
                         orchestrationType,
                         instanceId,
                         input,
                         stati);
                duplicate = false;

                await Task.WhenAny(timeoutTask, startTask);

                if (!startTask.IsCompleted)
                {
                    Trace.TraceWarning("TestProgress: Timed out {0} id={1} failed to start after {2}",
                                        orchestrationType.Name,
                                        instanceId,
                                        timeout);

                    throw new TimeoutException($"Orchestration {orchestrationType.Name} id={instanceId} failed to start after {timeout}.");
                }

                await startTask;
            }
            catch (InvalidOperationException e) when (e.Message.Contains("already exists"))
            {
                var instance = new OrchestrationInstance() { InstanceId = instanceId };
                var state = await this.client.GetOrchestrationStateAsync(instance);
                creationTime = state.CreatedTime;
                duplicate = true;
            }

            Trace.TraceInformation($"TestProgress: Started {orchestrationType.Name} id={instanceId} duplicate={duplicate}");
            return (!duplicate, new TestOrchestrationClient(this.client, orchestrationType, instanceId, creationTime));
        }

        public async Task<TestOrchestrationClient> StartOrchestrationWithRetriesAsync(
           TimeSpan timeout,
           TimeSpan period,
           Type orchestrationType,
           object input,
           string instanceId = null)
        {
            this.AddTypes(orchestrationType);
            instanceId = instanceId ?? Guid.NewGuid().ToString("n");
            Stopwatch sw = Stopwatch.StartNew();
            bool created = false;
            TestOrchestrationClient client = null;

            do
            {
                var periodTask = Task.Delay(period);
                async Task Invoke()
                {
                    (created, client) = await this.StartDeduplicatedOrchestrationAsync(orchestrationType, input, instanceId);
                }
                var invokeTask = Invoke();
                var firstTask = await Task.WhenAny(invokeTask, periodTask);
                if (firstTask == invokeTask)
                {
                    await invokeTask;
                }
            } while (client == null && (sw.Elapsed < timeout));

            return client;
        }

        readonly OrchestrationStatus[] dedupeStatuses = { OrchestrationStatus.Completed, OrchestrationStatus.Terminated, OrchestrationStatus.Pending, OrchestrationStatus.Running, OrchestrationStatus.Failed };

        public async Task<IList<OrchestrationState>> GetAllOrchestrationInstancesAsync()
        {
            // This API currently only exists in the service object and is not yet exposed on the TaskHubClient
            Trace.TraceInformation($"TestProgress: Querying all instances...");
            var instances = await ((IOrchestrationServiceQueryClient)this.orchestrationService).GetAllOrchestrationStatesAsync(CancellationToken.None);
            Trace.TraceInformation($"TestProgress: Found {instances.Count} in the task hub instance store.");
            return instances;
        }

        public async Task PurgeAllAsync()
        {
            Trace.TraceInformation($"TestProgress: Purging all instances...");
            var purgeResult = await ((IOrchestrationServiceQueryClient)this.orchestrationService).PurgeInstanceHistoryAsync(default, default, null);
            Trace.TraceInformation($"TestProgress: Purged {purgeResult} instances.");
        }

        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(DateTime? CreatedTimeFrom = default,
                                                                                DateTime? CreatedTimeTo = default,
                                                                                IEnumerable<OrchestrationStatus> RuntimeStatus = default,
                                                                                string InstanceIdPrefix = default,
                                                                                CancellationToken CancellationToken = default)
        {
            // This API currently only exists in the service object and is not yet exposed on the TaskHubClient
            Trace.TraceInformation($"TestProgress: Querying instances...");
            var instances = await ((IOrchestrationServiceQueryClient)this.orchestrationService).GetOrchestrationStateAsync(CreatedTimeFrom, CreatedTimeTo, RuntimeStatus, InstanceIdPrefix, CancellationToken);
            Trace.TraceInformation($"TestProgress: Found {instances.Count} in the task hub instance store.");
            return instances;
        }
    }
}
