<img align="right" src="src/DurableTask.Netherite/icon.png"/>

# Netherite

Netherite is a distributed workflow execution engine for [Durable Functions](https://github.com/Azure/azure-functions-durable-extension) (DF) and the [Durable Task Framework](https://github.com/Azure/durabletask/) (DTFx).

It is of potential interest to anyone developing applications on those platforms who has an appetite for performance, scalability, and reliability.

As Netherite is intended to be a drop-in backend replacement, it does not modify the application API. Existing DF and DTFx applications can switch to this backend with little effort.

## Getting Started

To get started, you can either try out the sample, or take an existing DF app and switch it to the Netherite backend. You can also read our [documentation](https://microsoft.github.io/durabletask-netherite/#/README).

**The hello sample.**

For a comprehensive quick start on using Netherite with Durable Functions, take a look at [hello sample walkthrough](https://microsoft.github.io/durabletask-netherite/#/hello-sample.md), and the associated [video content](https://microsoft.github.io/durabletask-netherite/#/hello-sample.md?id=walk-through-on-youtube-%f0%9f%8e%a5).
We included several scripts that make it easy to build, run, and deploy this application, both locally and in the cloud.
Also, this sample is a great starting point for creating your own projects.
 
**Configure an existing Durable Functions app for Netherite.**

If you have a .NET Durable Functions application already, and want to configure it to use Netherite as the backend, do the following:

- Add the NuGet package `Microsoft.Azure.DurableTask.Netherite.AzureFunctions` to your functions project (if using .NET) or your extensions project (if using TypeScript or Python).
- Add `"type" : "Netherite"` to the `storageProvider` section of your host.json. See [recommended host.json settings](https://microsoft.github.io/durabletask-netherite/#/settings.md).
- Configure your function app to run on 64 bit, if not already the case. You can do this in the Azure portal, or using the Azure CLI. Netherite does not run on 32 bit.
- Create an EventHubs namespace. You can do this in the Azure portal, or using the Azure CLI.
- Configure `EventHubsConnection` with the connection string for the Event Hubs namespace. You can do this using an environment variable, or with a function app configuration settings.

For more information, see the 
[.NET sample](https://github.com/microsoft/durabletask-netherite/tree/dev/samples/Hello_Netherite_with_DotNetCore), the 
[Python sample](https://github.com/microsoft/durabletask-netherite/tree/dev/samples/Hello_Netherite_with_Python), or the 
[TypeScript sample](https://github.com/microsoft/durabletask-netherite/tree/dev/samples/Hello_Netherite_with_TypeScript).

**Configure an existing Durable Task Application for Netherite.**

If you have an application that uses the Durable Task Framework already, and want to configure it to use Netherite as the backend, do the following:

- Create an EventHubs namespace. You can do this in the Azure portal, or using the Azure CLI.
- Add the NuGet package `Microsoft.Azure.DurableTask.Netherite` to your project.
- Update the server startup code to construct a `NetheriteOrchestrationService` object with the required settings, and then pass it as an argument to the constructors of `TaskHubClient` and `TaskHubWorker`.

For more information, see the [DTFx sample](https://github.com/microsoft/durabletask-netherite/blob/dev/samples/HelloDTFx/HelloDTFx/Program.cs).

## Why a new engine?

The default Azure Storage engine stores messages in Azure Storage queues and instance states in Azure Storage tables. 
It executes large numbers of small storage accesses. 
For example, executing a single orchestration with three activities may require a total of 4 dequeue operations, 3 enqueue operations, 4 table reads, and 4 table writes. 
Thus, the overall throughput quickly becomes limited by how many I/O operations Azure Storage allows per second.

To achieve better performance, Netherite represents queues and partition states differently, to improve batching:

- Partitions communicate via ordered streams, using EventHubs.
- The state of a partition is stored using a combination of an immutable log and checkpoints, in Azure PageBlobs.

To learn more about the Netherite architecture, our [VLDB 2022 paper](https://www.microsoft.com/en-us/research/uploads/prod/2022/07/p1591-burckhardt.pdf) is the best reference. 
There is also an earlier preprint [paper on arXiv](https://arxiv.org/abs/2103.00033).

For some other considerations about how to choose the engine, see [the documentation](https://microsoft.github.io/durabletask-netherite/#/engine.md).

## Status

The current version of Netherite is *1.1.1*. Netherite supports almost all of the DT and DF APIs. However, there are still some limitations:

- **Supported hosted plans**. Consumption plan is not supported yet, and auto-scaling only works on Elastic Premium plans with runtime-scaling enabled. This will be resolved by GA.
- **Query Performance**. Currently, queries do not support paging. We plan to add a range index implementation to fix this soon after GA.

## Contributing

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
