// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using DurableTask.Core;
using DurableTask.Netherite;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;


// ----------- construct the Netherite orchestration service

Console.WriteLine("Starting Netherite...");

var netheriteSettings = new NetheriteOrchestrationServiceSettings()
{
    HubName = "myhub",
    PartitionCount = 4,

    StorageConnectionName = "MyStorageConnection",
    EventHubsConnectionName = "MyEventHubsConnection",
};

netheriteSettings.Validate(connectionStringResolver);

// we use pseudo-connection strings here for demonstration purposes.
// See the sample "TokenCredentialDTFX" if you want to use Azure token credentials instead.
string connectionStringResolver(string connectionName)
{
    switch (connectionName)
    {
        case "MyStorageConnection":
            return "UseDevelopmentStorage=true;"; // use the local storage emulater
        case "MyEventHubsConnection":
            return "SingleHost"; // run Netherite in SingleHost mode, which does not require an Azure Event Hubs namespace resource
        default:
            throw new ArgumentException("invalid connection name");
    }
};

var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddSimpleConsole(options => options.SingleLine = true);
});

NetheriteOrchestrationService netherite = new NetheriteOrchestrationService(netheriteSettings, loggerFactory);


// ----------  create the task hub in storage, if it does not already exist

await ((IOrchestrationService) netherite).CreateIfNotExistsAsync();


// ---------- configure and start the DTFx worker

var worker = new TaskHubWorker(netherite, loggerFactory);

worker.AddTaskOrchestrations(typeof(HelloSequence));
worker.AddTaskActivities(typeof(SayHello));

await worker.StartAsync();


// ---------- configure the taskhub client

var client = new TaskHubClient(netherite);


// --------- start the orchestration, then wait for it to complete

Console.WriteLine("Starting the orchestration...");

OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(typeof(HelloSequence), null);

Console.WriteLine("Waiting for completion...");

OrchestrationState taskResult = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(30), CancellationToken.None);

Console.WriteLine($"Result:\n{JsonConvert.SerializeObject(taskResult, Formatting.Indented)}\n");


// --------- shut down the service

Console.WriteLine("Press any key to shut down...");
Console.ReadKey();

Console.WriteLine($"Shutting down...");

await worker.StopAsync();

Console.WriteLine("Done.");
