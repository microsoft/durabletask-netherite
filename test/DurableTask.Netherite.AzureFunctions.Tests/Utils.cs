// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.AzureFunctions.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

    static class Utils
    {
        // TODO: Make this a built-in API
        public static async Task<DurableOrchestrationStatus> WaitForCompletionAsync(
            this IDurableOrchestrationClient client,
            string instanceId,
            TimeSpan timeout)
        {
            using CancellationTokenSource cts = new CancellationTokenSource(timeout);
            return await client.WaitForCompletionAsync(instanceId, cts.Token);
        }

        // TODO: Make this a built-in API
        public static async Task<DurableOrchestrationStatus> WaitForCompletionAsync(
            this IDurableOrchestrationClient client,
            string instanceId,
            CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                DurableOrchestrationStatus status = await client.GetStatusAsync(instanceId);
                switch (status?.RuntimeStatus)
                {
                    case OrchestrationRuntimeStatus.Canceled:
                    case OrchestrationRuntimeStatus.Completed:
                    case OrchestrationRuntimeStatus.Failed:
                    case OrchestrationRuntimeStatus.Terminated:
                        return status;
                }

                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            cancellationToken.ThrowIfCancellationRequested();

            // Code should never reach here
            return null!;
        }
    }
}
