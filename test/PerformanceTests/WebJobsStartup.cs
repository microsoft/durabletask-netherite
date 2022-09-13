// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// Reference: https://docs.microsoft.com/en-us/azure/azure-functions/functions-dotnet-dependency-injection
[assembly: Microsoft.Azure.WebJobs.Hosting.WebJobsStartup(typeof(DurableTask.Netherite.AzureFunctions.OrleansStartup))]

namespace DurableTask.Netherite.AzureFunctions
{
    using System;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.DurableTask;
    using Microsoft.Azure.WebJobs.Hosting;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using PerformanceTests.Transport;

    class OrleansStartup : IWebJobsStartup
    {
        public void Configure(IWebJobsBuilder builder)
        {
            builder.Services.AddSingleton<ITransportLayerFactory, TriggerTransportFactory>();
            builder.Services.AddSingleton<TriggerTransportFactory, TriggerTransportFactory>();
        }
    }
}
