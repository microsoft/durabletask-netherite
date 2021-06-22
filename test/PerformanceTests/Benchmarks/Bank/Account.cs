// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace PerformanceTests.Bank
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using System.Collections.Generic;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using System.Linq;

    public interface IAccount
    {
        Task Add(int amount);
        Task Reset();
        Task<int> Get();
        void Delete();
    }

    [JsonObject(MemberSerialization.OptIn)]
    public class Account : IAccount
    {
        [JsonProperty("value")]
        public int Value { get; set; }

        public Task Add(int amount)
        {
            this.Value += amount;
            return Task.CompletedTask;
        }

        public Task Reset()
        {
            this.Value = 0;
            return Task.CompletedTask;
        }

        public Task<int> Get()
        {
            return Task.FromResult(this.Value);
        }

        public void Delete()
        {
            Entity.Current.DeleteState();
        }

        [FunctionName(nameof(Account))]
        public static Task Run([EntityTrigger] IDurableEntityContext context) => context.DispatchAsync<Account>();
    }
}
