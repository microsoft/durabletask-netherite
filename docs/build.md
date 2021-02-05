# Build

The repository contains source, tests, and samples. To build it, you need  [.NET Core SDK 3.1](https://dotnet.microsoft.com/download/dotnet-core/3.1). 
You can build the projects indivindually, or all at once. The Visual Studio solution `Netherite.sln` contains all of the projects.

## NuGet Packages

For most users there is no need to build Netherite from source. Instead, one can simply import one of the published NuGet packages:

|Target|NuGet Package Name|Project Name|
|------------|-------|---|
|Durable Functions applications |    Microsoft.Azure.DurableTask.Netherite.AzureFunctions | DurableTask.Netherite.AzureFunctions |  
| Durable Task applications| Microsoft.Azure.DurableTask.Netherite  | DurableTask.Netherite      |

## Dependencies

The following GitHub projects provide essential functionality for Netherite. All the necessary packages are automatically imported via NuGet.

* [Durable Task Framework](https://github.com/Azure/durabletask/) (DTFx). Provides the fundamental abstractions and APIs for the workflows-as-code computation model. For examples, it defines the concepts of *instances*, *orchestrations*, *activities*, and *histories*. It also includes an provider model that allows swapping the backend. Specifically, Netherite defines a class `NetheriteOrchestrationService` that implement the interface `DurableTask.Core.IOrchestrationService`.
* [Durable Functions Extension](https://github.com/Azure/azure-functions-durable-extension) (DF).
Provides the "glue" that makes it possible to run DTFx workflows on a serverless hosting platform. Specifically, it allows applications to define "Durable Functions", which can then be deployed to any context that can host Azure Functions. This can include managed hosting platforms (with consumption, premium, or dedicated plans), local development environments, or container-based platforms. DF also extends the DTFx programming model with support for Durable Entities and critical sections, and adds support for language bindings other than C#.
* [FASTER Log and KV](https://github.com/Microsoft/FASTER). Provides the storage technology that Netherite uses for durably persisting instance states. It includes both a log abstraction, which Netherite uses to write a recovery log, and a KV abstraction, which Netherite uses to store the state of orchestration instances. Compared to a simple implementation using tables or blobs, FASTER can provide superior throughput because it can efficiently aggregate small storage accesses into a smaller number of larger operations on "storage devices", which are currently backed by Azure Page Blobs.

Netherite also depends on Azure Storage, to store the taskhub data, and Azure EventHubs, to provide durable persistent queues for clients and partitions. In the future Netherite may support alternate storage or transport providers.