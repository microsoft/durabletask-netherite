// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Newtonsoft.Json;
    using Xunit;

    class TestOrchestrationClient
    {
        readonly TaskHubClient client;
        readonly Type orchestrationType;
        readonly string instanceId;
        readonly DateTime instanceCreationTime;

        public TestOrchestrationClient(
            TaskHubClient client,
            Type orchestrationType,
            string instanceId,
            DateTime instanceCreationTime)
        {
            this.client = client;
            this.orchestrationType = orchestrationType;
            this.instanceId = instanceId;
            this.instanceCreationTime = instanceCreationTime;
        }

        public string InstanceId => this.instanceId;

        public async Task<OrchestrationState> WaitForCompletionAsync(TimeSpan timeout, bool tolerateTimeout = false)
        {
            timeout = AdjustTimeout(timeout);

            var latestGeneration = new OrchestrationInstance { InstanceId = this.instanceId };
            Stopwatch sw = Stopwatch.StartNew();
            OrchestrationState state = await this.client.WaitForOrchestrationAsync(latestGeneration, timeout);
            if (state != null)
            {
                Trace.TraceInformation(
                    "TestProgress: Completed {0} id={1} after ~{2}ms. Status = {3}. Output = {4}.",
                    this.orchestrationType.Name,
                    state.OrchestrationInstance.InstanceId,
                    sw.ElapsedMilliseconds,
                    state.OrchestrationStatus,
                    state.Output);
            }
            else
            {
                Trace.TraceWarning(
                    "TestProgress: Timed out {0} id={1} failed to complete after {2}ms.",
                    this.orchestrationType.Name,
                    this.instanceId,
                    timeout.TotalMilliseconds);

                if (!tolerateTimeout)
                {
                    throw new TimeoutException($"Orchestration {this.orchestrationType.Name} id={this.instanceId} timed out after {timeout}.");
                }
            }

            return state;
        }

        public async Task<OrchestrationState> WaitForCompletionWithRetriesAsync(TimeSpan timeout, TimeSpan period, bool tolerateTimeout = false)
        {
            timeout = AdjustTimeout(timeout);
            period = AdjustTimeout(period);

            var latestGeneration = new OrchestrationInstance { InstanceId = this.instanceId };
            Stopwatch sw = Stopwatch.StartNew();
            OrchestrationState state = null;

            do
            {
                state = await this.client.WaitForOrchestrationAsync(latestGeneration, period);

            } while (state == null && sw.Elapsed < timeout);

            if (state != null)
            {
                Trace.TraceInformation(
                    "TestProgress: Completed {0} id={1} after ~{2}ms. Status = {3}. Output = {4}.",
                    this.orchestrationType.Name,
                    state.OrchestrationInstance.InstanceId,
                    sw.ElapsedMilliseconds,
                    state.OrchestrationStatus,
                    state.Output);
            }
            else
            {
                Trace.TraceWarning(
                    "TestProgress: Timed out {0} id={1} failed to complete with retries after {2}ms.",
                    this.orchestrationType.Name,
                    this.instanceId,
                    timeout.TotalMilliseconds);

                if (!tolerateTimeout)
                {
                    throw new TimeoutException($"Orchestration {this.orchestrationType.Name} id={this.instanceId} timed out after {timeout}.");
                }
            }

            return state;
        }

        internal async Task<OrchestrationState> WaitForStartupAsync(TimeSpan timeout, bool tolerateTimeout = false)
        {
            timeout = AdjustTimeout(timeout);

            Stopwatch sw = Stopwatch.StartNew();
            do
            {
                OrchestrationState state = await this.GetStatusAsync();
                if (state != null && state.OrchestrationStatus != OrchestrationStatus.Pending)
                {
                    Trace.TraceInformation($"TestProgress: Started {state.Name} id={state.OrchestrationInstance.InstanceId} after ~{sw.ElapsedMilliseconds}ms. Status = {state.OrchestrationStatus}.");
                    return state;
                }

                await Task.Delay(TimeSpan.FromSeconds(1));

            } while (sw.Elapsed < timeout);

            Trace.TraceWarning(
              "TestProgress: Timed out {0} id={1} failed to start after {2}ms.",
              this.orchestrationType.Name,
              this.instanceId,
              timeout.TotalMilliseconds);

            if (!tolerateTimeout)
            {
                throw new TimeoutException($"Orchestration {this.orchestrationType.Name} id={this.instanceId} failed to start.");
            }

            return null;
        }

        public async Task<OrchestrationState> GetStatusAsync()
        {
            OrchestrationState state = await this.client.GetOrchestrationStateAsync(this.instanceId);

            if (state != null)
            {
                // Validate the status before returning
                Assert.Equal(this.orchestrationType.FullName, state.Name);
                Assert.Equal(this.instanceId, state.OrchestrationInstance.InstanceId);
                Assert.True(state.CreatedTime >= this.instanceCreationTime);
                Assert.True(state.CreatedTime <= DateTime.UtcNow);
                Assert.True(state.LastUpdatedTime >= state.CreatedTime);
                Assert.True(state.LastUpdatedTime <= DateTime.UtcNow);
            }

            return state;
        }

        public Task RaiseEventAsync(string eventName, object eventData)
        {
            Trace.TraceInformation($"Raising event to instance {this.instanceId} with name = {eventName}.");

            var instance = new OrchestrationInstance { InstanceId = this.instanceId };
            return this.client.RaiseEventAsync(instance, eventName, eventData);
        }

        public Task TerminateAsync(string reason)
        {
            Trace.TraceInformation($"Terminating instance {this.instanceId} with reason = {reason}.");

            var instance = new OrchestrationInstance { InstanceId = this.instanceId };
            return this.client.TerminateInstanceAsync(instance, reason);
        }

        public Task RewindAsync(string reason)
        {
            Trace.TraceInformation($"Rewinding instance {this.instanceId} with reason = {reason}.");

            // The Rewind API is not implemented yet on the Netherite backend
            throw new NotImplementedException();
        }

        public Task PurgeInstanceHistory()
        {
            Trace.TraceInformation($"Purging history for instance with id - {this.instanceId}");

            var instance = new OrchestrationInstance { InstanceId = this.instanceId };
            var service = (NetheriteOrchestrationService)this.client.ServiceClient;
            return service.PurgeInstanceHistoryAsync(this.instanceId);
        }

        public Task PurgeInstanceHistoryByTimePeriod(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            Trace.TraceInformation($"Purging history from {createdTimeFrom} to {createdTimeTo}");

            var service = (NetheriteOrchestrationService)this.client.ServiceClient;
            return service.PurgeInstanceHistoryAsync(createdTimeFrom, createdTimeTo, runtimeStatus);
        }

        public async Task<List<HistoryStateEvent>> GetOrchestrationHistoryAsync(string instanceId)
        {
            Trace.TraceInformation($"Getting history for instance with id - {this.instanceId}");

            var service = (NetheriteOrchestrationService)this.client.ServiceClient;
            var state = await service.GetOrchestrationStateAsync(instanceId, false, false);
            if (state == null)
            {
                return new List<HistoryStateEvent>();
            }
            else
            {
                string historyString = await ((IOrchestrationServiceClient)service).GetOrchestrationHistoryAsync(instanceId, state.OrchestrationInstance.ExecutionId);
                return JsonConvert.DeserializeObject<List<HistoryStateEvent>>(historyString);
            }
        }

        public Task<IList<OrchestrationState>> GetStateAsync(string instanceId)
        {
            Trace.TraceInformation($"Getting orchestration state with instance id - {this.instanceId}");

            var service = (IOrchestrationServiceClient)this.client.ServiceClient;
            return service.GetOrchestrationStateAsync(instanceId, true);
        }

        internal static TimeSpan AdjustTimeout(TimeSpan requestedTimeout)
        {
            TimeSpan timeout = requestedTimeout;
            if (Debugger.IsAttached)
            {
                TimeSpan debuggingTimeout = TimeSpan.FromMinutes(5);
                if (debuggingTimeout > timeout)
                {
                    timeout = debuggingTimeout;
                }
            }

            timeout = timeout + TimeoutAdjustment;

            return timeout;
        }

        static TimeSpan TimeoutAdjustment = TimeSpan.Zero;

        public static IDisposable WithExtraTime(TimeSpan adjustment)
        {
            TimeoutAdjustment = adjustment;
            return new ResetTimeoutAdjust();
        }

        class ResetTimeoutAdjust : IDisposable
        {
            public void Dispose()
            {
                TimeoutAdjustment = TimeSpan.Zero;
            }
        }
    }
}
