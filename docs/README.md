# Netherite: Introduction

Netherite is a distributed workflow execution engine for [Durable Functions](https://github.com/Azure/azure-functions-durable-extension) and the [Durable Task Framework](https://github.com/Azure/durabletask/). It is of potential interest to anyone who is developing durable workflow or actor applications on those platforms. Its application API surface is the same, so applications can be ported with modest effort.

## Which engine to choose

The Durable Functions and Durable Task frameworks already support a multitude of execution engines. Choosing the right one depends on many factors that are specific to the intended scenario. If the following factors are important to you, Netherite is likely to be a good choice:

1. **Strong consistency**. Netherite combines the reliable in-order delivery provided by EventHubs with the reliable log-and-checkpoint persistence provided by FASTER. All data stored in the task hub is transactionally consistent, minimizing the chance of duplicate execution that is more common in eventually-consistent storage providers.

1. **High throughput**.  Netherite is designed to handle high-scale situations:
    - Communication via EventHubs can accommodate high throughput requirements.
    - Progress is committed to storage in batches, which improves throughput compared to storage providers that need to issue individual small I/O operations for each orchestration step.
    - The in-order delivery guarantee of EventHubs allows entity signals to be streamed more efficiently.
1. **Low latency**. Netherite also includes several latency optimizations:
    - EventHubs supports long polling. This improves latency compared to standard polling on message queues, especially in low-scale situations when polling intervals are high.
    - Clients can connect to Netherite via a bidirectional EventHubs connection. This means that a client waiting for an orchestration to complete is notified more quickly.
    - Netherite caches all instance and entity states in memory, which improves latency if they are accessed repeatedly.
    - Multiple orchestration steps can be persisted in a single storage write, which reduces latency when issuing a sequence of very short activities.

## Status

The current version of Netherite is *0.1.0-alpha*.  Netherite already support almost all of the DT and DF APIs. However, there are still some limitations that we plan to address in the near future, before moving to beta status:

- **Auto-Scaling**. While it is already possible to dynamically change the number of host nodes (and have Netherite rebalance load automatically), support for doing so automatically is not implemented yet.
- **Query Performance**. We have not quite completed our implementation of a FASTER index to speed up queries that are enumerating or purging instance states.
- **Stability**. We do not recommend using Netherite in a production environment yet; although we have found and fixed many bugs already, we need more testing before moving to beta status. Any help from the community is greatly appreciated!
