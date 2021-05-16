// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Bank
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;

    /// <summary>
    /// A microbenchmark using durable entities for bank accounts, and an orchestration with a critical section for transferring
    /// currency between two accounts.
    /// </summary>
    public static class BankTransaction
    {
        [FunctionName(nameof(BankTransaction))]
        public static async Task<bool> Run([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var iterationLength = context.GetInput<Tuple<int, int>>();
            var targetAccountPair = iterationLength.Item1;
            var length = iterationLength.Item2;

            var sourceAccountId = $"src{targetAccountPair}-!{(targetAccountPair + 1) % 32:D2}";
            var sourceEntity = new EntityId(nameof(Account), sourceAccountId);

            var destinationAccountId = $"dst{targetAccountPair}-!{(targetAccountPair + 2) % 32:D2}";
            var destinationEntity = new EntityId(nameof(Account), destinationAccountId);

            // Add an amount to the first account
            var transferAmount = 1000;

            IAccount sourceProxy =
                context.CreateEntityProxy<IAccount>(sourceEntity);
            IAccount destinationProxy =
                context.CreateEntityProxy<IAccount>(destinationEntity);

            // we want the balance check to always succeeed to reduce noise
            bool forceSuccess = true;

            // Create a critical section to avoid race conditions.
            // No operations can be performed on either the source or
            // destination accounts until the locks are released.
            using (await context.LockAsync(sourceEntity, destinationEntity))
            {
                int sourceBalance = await sourceProxy.Get();

                if (sourceBalance > transferAmount || forceSuccess)
                {
                    await sourceProxy.Add(-transferAmount);
                    await destinationProxy.Add(transferAmount);

                    return true;
                }
                else
                {
                    return false;
                }
            }
        }
    }
}
