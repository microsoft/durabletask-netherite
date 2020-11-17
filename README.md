# DurableTask-Netherite

## Introduction

DurableTask.Netherite is a backend storage provider for the Durable Task Framework (DTFx).


**(TODO)**

## Build and Dependencies

The solution `DurableTask.Netherite.sln` builds all the projects. This includes source, tests, and samples. It requires [.NET Core 3.1 SDK](https://dotnet.microsoft.com/download/dotnet-core/3.1). All dependencies are imported via NuGet.

Netherite depends on the following GitHub projects:
* [Durable Task Framework](https://github.com/Azure/durabletask/)
* [Durable Functions Extension](https://github.com/Azure/azure-functions-durable-extension)
* [FASTER Log and KV](https://github.com/Microsoft/FASTER)

The current implementation of Netherite also depends on Azure Storage, to store the taskhub data, and Azure EventHubs, to provide durable persistent queues for clients and partitions. In the future Netherite may support alternate providers.

## Getting Started

We now give instructions for running the sample project locally and in the cloud.

### Run the 'Hello' Sample Locally, Emulated

If you have the Azure Storage Emulator installed, you can run the sample locally, in "emulation mode". To do so, set the following environment variables before building and running `samples/Hello/Hello.csproj`:

```shell
set AzureWebJobsStorage=UseDevelopmentStorage=true;
set EventHubsConnection=MemoryF:12
```
If you don't have the Azure Storage Emulator, just replace `UseDevelopmentStorage=true;` with an actual connection string to an Azure Storage account.

Note that if working with Visual Studio, you can just hit the run button since the Hello sample is set as the startup project by default. You may have to restart VS if you changed the environment variables after opening VS.

Once the Functions runtime has started successfully, you can run the orchestration by issuing an http request:

```shell
curl http://localhost:7071/api/hellocities
```

### Run the 'Hello' Sample Locally, with EventHubs

To use EventHubs instead of emulation, you first have to create an EventHubs namespace.
You can do this by using the Azure portal GUI and creating the necessary structure as described in the section `Configuring EventHubs` below. Or quicker, edit and run the included script `scripts/create-eventhubs-instances.ps1`, which automatically creates a resource group, namespace, and event hubs using Azure CLI commands.

After you have created the EventHubs, visit the Azure management portal, and under Shared Access Policy, retrieve the connection string for the Eventhubs namespace. Then assign it to the environment variable, like this

```shell
set EventHubsConnection=Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=...
```

Now run the Hello sample again. It uses EventHubs for connecting the client and the partitions.

### Run the 'Hello' Sample on a Premium Plan

When running Netherite in the cloud, a premium plan with fixed minimum node count is recommended for now, until we implement autoscaling.  

To publish the Hello Sample to an Azure Function App, you have to create various Azure resources. To make things simple for this quick start, the Hello sample includes a script that creates and configures a function app, and then publishes the hello function app. To use it,

1. Make sure the environment variable `EventHubsConnection` contains the desired value
1. Edit the name parameters in the script `deploy-to-new-plan.ps1` in the Hello project
1. build the Hello project
1. Go to the directory with the binaries, e.g. `cd samples\Hello\bin\Debug\netcoreapp3.1\`
1. run the script there, e.g. by typing `deploy-to-new-plan.ps1` in a shell

after the script runs successfully, near the bottom of the output, you should see a list of all published functions:

```
 Functions in <nameofyourfunctionapp>:
    HelloCities - [httpTrigger]
        Invoke url: https://<nameofyourfunctionapp>.azurewebsites.net/api/hellocities

    HelloSequence - [orchestrationTrigger]

    SayHello - [activityTrigger]
```

You can now test the app by issuing an HTTP request to the URL listed for HelloCities, i.e. 

```shell
curl https://<nameofyourfunctionapp>.azurewebsites.net/api/hellocities
```


## EventHubs Configuration

To run on Netherite, one must configure an EventHubs namespace that connects clients and partitions using persistent queues. Moreover, this namespace must contain the following EventHubs:

* An event hub called `partitions` with 1-32 partitions. We recommend to stay within the range of 12-32.
* Four event hubs called `clients0`, `clients1`, `clients2` and `clients3` with 32 partitions each.

You can create these manually in the Azure portal, or via the Azure CLI:

```shell
az eventhubs eventhub create --namespace-name <namepace> --resource-group <group> --name partitions --message-retention 1 --partition-count 12
az eventhubs eventhub create --namespace-name <namepace> --resource-group <group> --name clients0 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name <namepace> --resource-group <group> --name clients1 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name <namepace> --resource-group <group> --name clients2 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name <namepace> --resource-group <group> --name clients3 --message-retention 1 --partition-count 32
```

You can also edit and run the script `create-eventhubs-instances.ps1` to this end.  

### Multiple TaskHubs

If you use multiple task hubs over time, but not at the same time, you can reuse the same EventHubs namespace. In particular, it is not necessary to clear the contents in between runs.

Running concurrent TaskHubs on the same EventHubs namespace is however not recommmended. In that case, each TaskHub should have its own EventHubs namespace.

### Partition Count Considerations

Setting the partition count to 12 means the system will operate smoothly both for small loads and moderately heavy loads with a scale-out of up to 12 nodes. Setting it to 32 nodes achieves maximum throughput under heavy load, with a scale-out of up to 32 nodes, but can be a little bit less efficient under very small loads (e.g. when scaling to or from zero). We do not recommend setting it to less than 12 because this limits scale-out.

Note that in the current implementation, the partition count for an existing TaskHub cannot be changed.  

### Emulation Modes

For testing or debugging scenarios, we support an emulation mode that eliminates the need for using an actual EventHubs. Since communication is "simulated in memory", this makes sense only for a single node and offers no durability. The emulation mode can be selected by using a pseudo connection string of the form `Memory:n` or `MemoryF:n`, where n is a number in the range 1-32 indicating the number of partitions to use. The difference between the two is that the former emulates the storage also, while the latter exercises the Faster storage component normally.

# General Information

### Trademarks 

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow Microsoft's Trademark & Brand Guidelines. Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.

### Contributing

This project welcomes contributions and suggestions. Most contributions require you to
agree to a Contributor License Agreement (CLA) declaring that you have the right to,
and actually do, grant us the rights to use your contribution. For details, visit
https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need
to provide a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the
instructions provided by the bot. You will only need to do this once across all repositories using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
