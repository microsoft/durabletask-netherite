# Netherite: Introduction

Netherite is a distributed workflow execution engine for [Durable Functions](https://github.com/Azure/azure-functions-durable-extension) (DF) and the [Durable Task Framework](https://github.com/Azure/durabletask/) (DTFx). 

It is of potential interest to anyone developing applications on those platforms who has an appetite for performance, scalability, and reliability. 

?> Netherite is a drop-in replacement for the default backend, it does not modify the application API. Existing DF applications can switch to Netherite with little effort.

## Getting Started

To get started, you can either try out the sample, or take an existing DF app and switch it to the Netherite backend.

**The hello sample.**
For a quick start, take a look at [hello sample](hello-sample). We included scripts that make it easy to build, run, and deploy this application. Also, this sample is a great starting point for creating your own projects.

**Configure an existing DF app to use Netherite.**
If you have a .NET Durable Functions application already, and want to configure it to use Netherite as the backend, do the following:
- Add the NuGet package `Microsoft.Azure.DurableTask.Netherite.AzureFunctions` to your functions project.
- Create an EventHubs namespace. You can do this in the Azure portal, or using the Azure CLI.
- Configure `EventHubsConnection` with the connection string for the Event Hubs namespace. You can do this using an environment variable, or with a function app configuration settings.
- Add `"type" : "Netherite"` to the `storageProvider` section of your host.json. See [recommended host.json settings](settings).

!> **Important** Never use the same EventHubs namespace for multiple function apps at the same time.

## Why a new engine?

The default Azure Storage engine stores messages in Azure Storage queues and instance states in Azure Storage tables. It executes large numbers of small storage accesses. For example, executing a single orchestration with three activities may require a total of 4 dequeue operations, 3 enqueue operations, 4 table reads, and 4 table writes. Thus, the overall throughput quickly becomes limited by how many I/O operations Azure Storage allows per second. 

To achieve better performance, Netherite represents queues and partition states differently, to improve batching:
- Partitions communicate via ordered, persistent event streams, over EventHubs.
- The state of a partition is stored using a combination of an immutable log and checkpoints, in Azure PageBlobs.

Just as in the [previous architecture](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-perf-and-scale#orchestrator-scale-out), partitions are load-balanced at runtime over the available nodes. However, unlike before, Netherite partitions apply to both activities and orchestrations, so there is not a distinction between control queues and work-item queues.

The following picture illustrates the architecture, in a situation where five partitions are distributed over two nodes:

![Netherite Architecture](images/partitions.png)

Each partition is represented in storage using the FASTER database technology, which also provides basic indexing and querying. We discuss this in more detail in the section on [storage organization](storage?id=description-of-storage-content). 

Another advantage of this architecture is that we can store the current input queue position of a partition as part of the partition state. This is important in cases where we need to recover from a crash, or if we need to move a partition from one node to another. In those situation, the node that is restarting the partition can check the input position of the last processed message, and resume processing exactly where it left off. 

**Components.** Currently, Netherite relies on the following services:
- *EventHubs* provides the persistent queue service.
- *Azure Storage Page Blobs* provide the underlying raw storage for the logs.

In the future, we plan to support alternatives for these components. For example, Kafka instead of EventHubs, and K8s persistent volumes instead of Azure Page Blobs.

## Status

The current version of Netherite is *1.0.2*. Netherite supports almost all of the DT and DF APIs. However, there are still some limitations:

- **Supported hosted plans**. Consumption plan is not supported yet, and auto-scaling only works on Elastic Premium plans with runtime-scaling enabled. This will be resolved by GA.
- **Query Performance**. Currently, query performance is suboptimal and does not support paging. We plan to add a range index implementation to fix this soon after GA.

To learn more about the Netherite architecture, you can also check out our [paper on arXiv](https://arxiv.org/abs/2103.00033).
