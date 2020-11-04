// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

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

            if (TestHelpers.DeleteStorageBeforeRunningTests)
            {
                this.orchestrationService.DeleteAsync().GetAwaiter().GetResult();
            }

            this.orchestrationService.CreateAsync(false).GetAwaiter().GetResult();

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
            Trace.TraceInformation($"Started {this.orchestrationService}");

            await this.worker.StartAsync();
        }

        public Task StopAsync(bool isForced)
        {
            return this.worker.StopAsync(isForced);
        }

        public void AddAutoStartOrchestrator(Type type)
        {
            this.worker.AddTaskOrchestrations(new AutoStartOrchestrationCreator(type));
            this.addedOrchestrationTypes.Add(type);
        }

        public async Task<TestOrchestrationClient> StartOrchestrationAsync(
            Type orchestrationType,
            object input,
            string instanceId = null, 
            DateTime? startAt = null)
        {
            if (!this.addedOrchestrationTypes.Contains(orchestrationType))
            {
                this.worker.AddTaskOrchestrations(orchestrationType);
                this.addedOrchestrationTypes.Add(orchestrationType);
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

            DateTime creationTime = DateTime.UtcNow;
            OrchestrationInstance instance = startAt.HasValue ?
                await this.client.CreateScheduledOrchestrationInstanceAsync(
                    orchestrationType,
                    instanceId,
                    input,
                    startAt.Value)
                    :
                await this.client.CreateOrchestrationInstanceAsync(
                    orchestrationType,
                    instanceId,
                    input);


            Trace.TraceInformation($"Started {orchestrationType.Name}, Instance ID = {instance.InstanceId}");
            return new TestOrchestrationClient(this.client, orchestrationType, instance.InstanceId, creationTime);
        }

        public async Task<IList<OrchestrationState>> GetAllOrchestrationInstancesAsync()
        {
            // This API currently only exists in the service object and is not yet exposed on the TaskHubClient
            var instances = await this.orchestrationService.GetAllOrchestrationStatesAsync(CancellationToken.None);
            Trace.TraceInformation($"Found {instances.Count} in the task hub instance store.");
            return instances;
        }

        public async Task PurgeAllAsync()
        {
            Trace.TraceInformation($"Purging all instances...");
            var purgeResult = await this.orchestrationService.PurgeInstanceHistoryAsync(default, default, null);
            Trace.TraceInformation($"Purged {purgeResult} instances.");
        }

        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(DateTime? CreatedTimeFrom = default,
                                                                                DateTime? CreatedTimeTo = default,
                                                                                IEnumerable<OrchestrationStatus> RuntimeStatus = default,
                                                                                string InstanceIdPrefix = default,
                                                                                CancellationToken CancellationToken = default)
        {
            // This API currently only exists in the service object and is not yet exposed on the TaskHubClient
            var instances = await this.orchestrationService.GetOrchestrationStateAsync(CreatedTimeFrom, CreatedTimeTo, RuntimeStatus, InstanceIdPrefix, CancellationToken);
            Trace.TraceInformation($"Found {instances.Count} in the task hub instance store.");
            return instances;
        }
    }
}
