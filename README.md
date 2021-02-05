<p style="text-align: center; float: right;"><img src="src/DurableTask.Netherite/icon.png" /></p>

# Netherite

Netherite is a distributed workflow execution engine for [Durable Functions](https://github.com/Azure/azure-functions-durable-extension) (DF) and the [Durable Task Framework](https://github.com/Azure/durabletask/) (DTFx).

It is of potential interest to anyone developing applications on those platforms who has an appetite for performance, scalability, and reliability.

As Netherite is intended to be a drop-in backend replacement, it does not modify the application API. Existing DF and DTFx applications can switch to this backend with little effort.

## Getting Started

To get started, you can either try out the sample, or take an existing DF app and switch it to the Netherite backend. You can also read our [documentation](https://microsoft.github.io/durabletask-netherite/#/README).

**The hello sample.**
For a quick start, take a look at [hello sample](https://microsoft.github.io/durabletask-netherite/#/hello-sample.md). We included scripts that make it easy to build, run, and deploy this application. Also, this sample is a great starting point for creating your own projects.

**Configure an existing DF app to use Netherite.**
If you have a .NET Durable Functions application already, and want to configure it to use Netherite as the backend, do the following:

- Add the NuGet package `Microsoft.Azure.DurableTask.Netherite.AzureFunctions` to your functions project.
- Create an EventHubs namespace. You can do this in the Azure portal, or using the Azure CLI.
- Configure `EventHubsConnection` with the connection string for the Event Hubs namespace. You can do this using an environment variable, or with a function app configuration settings.
- Optionally, update the host.json to tweak the [settings for Netherite](https://microsoft.github.io/durabletask-netherite/#/settings.md).

## Why a new engine?

The default Azure Storage engine stores messages in Azure Storage queues and instance states in Azure Storage tables. It executes large numbers of small storage accesses. For example, executing a single orchestration with three activities may require a total of 4 dequeue operations, 3 enqueue operations, 4 table reads, and 4 table writes. Thus, the overall throughput quickly becomes limited by how many I/O operations Azure Storage allows per second.

To achieve better performance, Netherite represents queues and partition states differently, to improve batching:

- Partitions communicate via ordered, persistent event streams, over EventHubs.
- The state of a partition is stored using a combination of an immutable log and checkpoints, in Azure PageBlobs.

For some other considerations about how to choose the engine, see [the documentation](https://microsoft.github.io/durabletask-netherite/#/engine.md).

## Status

The current version of Netherite is *0.1.0-alpha*.  Netherite already support almost all of the DT and DF APIs. However, there are still some limitations that we plan to address in the near future, before moving to beta status:

- **Auto-Scaling**. While it is already possible to dynamically change the number of host nodes (and have Netherite rebalance load automatically), support for doing so automatically is not implemented yet.
- **Query Performance**. We have not quite completed our implementation of a FASTER index to speed up queries that are enumerating or purging instance states.
- **Stability**. We do not recommend using Netherite in a production environment yet; although we have found and fixed many bugs already, we need more testing before moving to beta status. Any help from the community is greatly appreciated!

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
