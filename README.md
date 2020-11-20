# DurableTask-Netherite

## Introduction

DurableTask.Netherite is a distributed workflow execution engine for [Durable Functions](https://github.com/Azure/azure-functions-durable-extension) and the [Durable Task Framework](https://github.com/Azure/durabletask/). It is of potential interest to anyone who is developing durable workflow or actor applications on those platforms. Its application API surface is the same as the standard DF API, so applications can be ported with modest effort.

### When to choose DurableTask.Netherite

The Durable Functions and Durable Task frameworks already support a multitude of storage providers. Choosing the right provider depends on many factors that are specific to the intended scenario. If the following factors are important to you, DurableTask.Netherite is likely to be a good choice:

1. **Strong consistency**. Netherite combines the reliable in-order delivery provided by EventHubs with the reliable log-and-checkpoint persistence provided by FASTER. All data stored in the task hub is transactionally consistent, minimizing the chance of duplicate execution that is more common in eventually-consistent storage providers.
2. **High throughput**.  Netherite is designed to handle high-scale situations:
    - Communication via EventHubs can accommodate high throughput requirements.
    - Progress is committed to storage in batches, which improves throughput compared to storage providers that need to issue individual small I/O operations for each orchestration step.
    - The in-order delivery guarantee of EventHubs allows entity signals to be streamed more efficiently.
3. **Low latency**. Netherite also includes several latency optimizations:
    - EventHubs supports long polling. This improves latency compared to standard polling on message queues, especially in low-scale situations when polling intervals are high.
    - Clients can connect to Netherite via a bidirectional EventHubs connection. This means that a client waiting for an orchestration to complete is notified more quickly.
    - Netherite caches all instance and entity states in memory, which improves latency if they are accessed repeatedly.
    - Multiple orchestration steps can be persisted in a single storage write, which reduces latency when issuing a sequence of very short activities.

## Status

The current version of Netherite is *0.1.0-alpha*.  Netherite already support almost all of DF's API. However, there are still some limitations that we plan to address in the near future, before moving to beta status:

- **Auto-Scaling**. While it is already possible to dynamically change the number of host nodes (and have Netherite rebalance load automatically), support for doing so automatically is not implemented yet. 
- **Query Performance**. We have not quite completed our implementation of a FASTER index to speed up queries that are enumerating or purging instance states.
- **Stability**. We do not recommend using Netherite in a production environment yet; although we have found and fixed many bugs already, we need more testing before moving to beta status. Any help from the community is greatly appreciated!

## Build and Dependencies

The solution `DurableTask.Netherite.sln` builds all the projects. This includes source, tests, and samples. It requires [.NET Core 3.1 SDK](https://dotnet.microsoft.com/download/dotnet-core/3.1). All dependencies are imported via NuGet. 

The following GitHub projects provide the essential components for Netherite:
* [Durable Task Framework](https://github.com/Azure/durabletask/) (DTFx). Provides the fundamental abstractions and APIs for the stateful serverless computation model. For examples, it defines the concepts of *instances*, *orchestrations*, *activities*, and *histories*. It also includes an provider model that allows swapping the backend. Specifically, Netherite defines a class `NetheriteOrchestrationService` that implement the interface `DurableTask.Core.IOrchestrationService`.
* [Durable Functions Extension](https://github.com/Azure/azure-functions-durable-extension) (DF).
Provides the "glue" that makes it possible to run DTFx workflows on a serverless hosting platform. Specifically, it allows applications to define "Durable Functions", which can then be deployed to any context that can host Azure Functions. This can include managed hosting platforms (with consumption, premium, or dedicated plans), local development environments, or container-based platforms. DF also extends the DTFx programming model with support for Durable Entities and critical sections, and adds support for language bindings other than C#.
* [FASTER Log and KV](https://github.com/Microsoft/FASTER). Provides the storage technology that Netherite uses for durably persisting instance states. It includes both a log abstraction, which Netherite uses to write a recovery log, and a KV abstraction, which Netherite uses to store the state of orchestration instances. Compared to a simple implementation using tables or blobs, FASTER can provide superior throughput because it can efficiently aggregate small storage accesses into a smaller number of larger operations on "storage devices", which are currently backed by Azure Page Blobs.

Netherite also depends on Azure Storage, to store the taskhub data, and Azure EventHubs, to provide durable persistent queues for clients and partitions. In the future Netherite may support alternate storage or transport providers.

## Getting Started

Below we include simple instructions for running the sample project locally and in the cloud. We assume basic familiarity with how to develop and run a DF application in C#, and with how to navigate the Azure Portal. To learn more about DF, see this [tutorial](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-create-first-csharp?WT.mc_id=email), for example.

### Run the 'Hello' Sample Locally, Emulated

If you have the Azure Storage Emulator installed, you can run the sample locally, in "emulation mode". To do so, set the following environment variables before building and running `samples/Hello/Hello.csproj`:

```shell
set AzureWebJobsStorage=UseDevelopmentStorage=true;
set EventHubsConnection=MemoryF:12
```
If you don't have the Azure Storage Emulator, you will need to use an Azure subscription and Azure Storage account. Just replace `UseDevelopmentStorage=true;` with the connection string to the Azure Storage account.

Note that if working with Visual Studio, you can just hit the run button since the Hello sample is set as the startup project by default. You may have to restart VS if you changed the environment variables after opening VS.

Once the Functions runtime has started successfully, you can run the orchestration by issuing an http request:

```shell
curl http://localhost:7071/api/hellocities
```

### Run the 'Hello' Sample Locally, with EventHubs

To use a real EventHubs, you first have to create an EventHubs namespace.
You can do this by using the Azure portal GUI and creating the necessary structure as described in the section `Configuring EventHubs` below. Or quicker, edit and run the included script `scripts/create-eventhubs-instances.ps1`, which automatically creates a resource group, namespace, and event hubs using Azure CLI commands. The script performs the following commands:

```PowerShell
# edit these parameters before running the script
$groupName="..."
$nameSpaceName="..."
$location='westus'
$partitionCount=12

Write-Host "Creating Resource Group..."
az group create --name $groupName  --location $location

Write-Host "Creating EventHubs Namespace..."
az eventhubs namespace create --name $nameSpaceName --resource-group $groupName 

Write-Host "Creating EventHubs partitions..."
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name partitions --message-retention 1 --partition-count $partitionCount
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients0 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients1 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients2 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients3 --message-retention 1 --partition-count 32
```

After you have created the EventHubs, visit the Azure management portal, and under Shared Access Policy, retrieve the connection string for the Eventhubs namespace. Then assign it to the environment variable, like this

```shell
set EventHubsConnection=Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=...
```

Now run the Hello sample again. It uses EventHubs for connecting the client and the partitions.

### Run the 'Hello' Sample in the Cloud

When running Netherite in the cloud, a *premium plan* with fixed minimum node count is recommended for now, until we implement autoscaling.  

To publish the Hello Sample to an Azure Function App, you have to create various Azure resources. To make things simple for this quick start, we made a PowerShell script that creates and configures a function app, and then publishes the hello function app. These are the relevant steps:

```powershell
# edit these parameters before running the script
$name="unique-alphanumeric-name-no-dashes"
$location="westus"
$storageSku="Standard_LRS"
$planSku="EP1"
$numNodes=2

# by default, use the same name for group, function app, storage account, and plan
$groupName=$name
$functionAppName=$name
$storageName=$name
$planName=$name

Write-Host "Creating Resource Group..."
az group create --name $groupName --location $location

Write-Host "Creating Storage Account"
az storage account create --name  $storageName --location $location --resource-group  $groupName --sku $storageSku

Write-Host "Creating and Configuring Function App"
az functionapp plan create --resource-group  $groupName --name  $name --location $location --sku $planSku
az functionapp create --name  $functionAppName --storage-account  $storageName --plan  $functionAppName --resource-group  $groupName --functions-version 3
az functionapp plan update -g  $groupName -n  $functionAppName --max-burst $numNodes --number-of-workers $numNodes --min-instances $numNodes 
az resource update -n  $functionAppName/config/web  -g  $groupName --set properties.minimumElasticInstanceCount=$numNodes --resource-type Microsoft.Web/sites
az functionapp config appsettings set -n $functionAppName -g  $groupName --settings EventHubsConnection=$Env:EventHubsConnection
az functionapp config set -n $functionAppName -g $groupName --use-32bit-worker-process false

Write-Host "Publishing Code to Function App"
func azure functionapp publish $functionAppName
```

To use the script for deploying the Hello sample,

1. Make sure the environment variable `EventHubsConnection` contains the desired value.
1. Edit the name parameters in the script `deploy-to-new-plan.ps1` in the Hello project.
1. Build the Hello project.
1. Go to the directory with the binaries, e.g. `cd samples\Hello\bin\Debug\netcoreapp3.1\`.
1. Run the script there, e.g. by typing `deploy-to-new-plan.ps1` in a shell.

After the script runs successfully, near the bottom of the output, you should see a list of all published functions:

```text
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

### Cleanup

Don't forget to delete EventHubs and Plan resources you no longer need, as they will continue to accrue charges.

## EventHubs Configuration

To run a Durable Functions application on Netherite, you must configure an EventHubs namespace that connects clients and partitions using persistent queues. This namespace must contain the following EventHubs:

* An event hub called `partitions` with 1-32 partitions. We recommend 12 as a default.
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

If you use multiple task hubs over time, but not at the same time, you can reuse the same EventHubs namespace. It is not necessary, nor desirable, to clear the contents of the EventHubs streams in between runs: any time a new TaskHub is created, it starts consuming and producing messages relative to the current stream positions. These initial positions are also recorded in the `taskhub.json` file.

Running concurrent TaskHubs on the same EventHubs namespace is not recommmended. In that case, each TaskHub should have its own EventHubs namespace.

### Partition Count Considerations

In the current implementation, the partition count for an existing TaskHub cannot be changed. Thus, some consideration for choosing this value intially is required.

Setting the partition count to 12 means the system will operate smoothly both for small loads and moderately heavy loads with a scale-out of up to 12 nodes. Setting it to 32 nodes achieves maximum throughput under heavy load, with a scale-out of up to 32 nodes, but can be a little bit less efficient under very small loads (for example, when scaling to or from zero). Unless there is certainty that the system does not need to scale out, we do not recommend setting it to less than 12.

Note that compared to the existing Durable Functions implementation, the partition count is similar to the control queue count; however, unlike the latter, it also affects the maximum scale out for activities (not just orchestrations). For applications that require massive scale for activities, we recommend using HttpTriggers in place of activities.

### Emulation Modes

For testing or debugging scenarios, we support an emulation mode that eliminates the need for using an actual EventHubs. Since communication is "simulated in memory", this makes sense only for a single node and offers no durability. The emulation mode can be selected by using a pseudo connection string of the form `Memory:n` or `MemoryF:n`, where n is a number in the range 1-32 indicating the number of partitions to use. The difference between the two is that the former emulates the storage also, while the latter exercises the Faster storage component normally.

## General Information

### Contributing

This project welcomes contributions and suggestions. Most contributions require you to
agree to a [Contributor License Agreement (CLA)](https://cla.microsoft.com) declaring that you have the right to, and actually do, grant us the rights to use your contribution. 

When you submit a pull request, a CLA-bot will automatically determine whether you need
to provide a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the
instructions provided by the bot. You will only need to do this once across all repositories using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

### Security

Microsoft takes the security of our software products and services seriously, which includes [Microsoft](https://github.com/Microsoft), [Azure](https://github.com/Azure), [DotNet](https://github.com/dotnet), [AspNet](https://github.com/aspnet), [Xamarin](https://github.com/xamarin), and [our GitHub organizations](https://opensource.microsoft.com/).

If you believe you have found a security vulnerability in any Microsoft-owned repository that meets Microsoft's [Microsoft's definition of a security vulnerability](https://docs.microsoft.com/en-us/previous-versions/tn-archive/cc751383(v=technet.10)), please report it to us at the Microsoft Security Response Center (MSRC) at [https://msrc.microsoft.com/create-report](https://msrc.microsoft.com/create-report). **Do not report security vulnerabilities through GitHub issues.**

### Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow Microsoft's Trademark & Brand Guidelines. Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.


