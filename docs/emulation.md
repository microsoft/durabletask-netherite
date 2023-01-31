# Emulation modes

For testing or debugging scenarios, Netherite supports two emulation modes.
The emulation mode can be selected by using a "pseudo-connection-string" instead of a real EventHubs connection string:

|String | Interpretation |
|--|--|
|Memory| simulate both the queues and the partition states in memory. |
|SingleHost| simulate the queues in memory, but store the partition states in Azure Storage |

## Limitations

Both of these modes work only in a single process on a single machine.
Specifically, they cannot be used to run the task hub on multiple machines, or using multiple processes on the same machine.
Also, they do not support using external clients that are running in separate processes.

## Memory mode

The `Memory` mode runs the entire [task hub](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-task-hubs) in memory.
This mode is well suited for testing and debugging of a function app as it executes fast and involves no cloud storage resources.
However, this also means that the state of messages, orchestrations, or entities is *not* persisted in any way - it is all lost when the functions host process is shut down.

## SingleHost mode

The `SingleHost` mode allows running the function app without an event hub resource.
The idea is that since all partitions are placed on a single host machine, the partitions can communicate directly without requiring an event hub intermediary.
Unlike the `Memory` mode, an application running in `SingleHost` mode still preserves all the task hub contents in Azure Storage.
It is in fact possible to switch back and forth between normal mode (with event hubs) and `SingleHost` mode (without event hubs) on the same task hub.

!> **Support in older versions** Netherite versions prior to 1.2.0 supported a more limited implementation of the `SingleHost` mode only, called `MemoryF`.
Unlike the `SingleHost` mode, the `MemoryF` mode does not preserve the task hub contents when switching modes.
In Netherite versions starting with 1.2.0, choosing the `MemoryF` mode is still supported for compatibility, but it is equivalent to choosing the `SingleHost` mode.

## Quick Summary of Configuration Steps

To configure a Durable Functions application to use the Netherite emulation modes:

1. Add the NuGet package `Microsoft.Azure.DurableTask.Netherite.AzureFunctions` to your function app project.
2. Set the `AzureWebJobsStorage` configuration parameter to use a suitable connection string, or local emulation (e.g. `set AzureWebJobsStorage=UseDevelopmentStorage=true;`).
3. Set the `EventHubsConnection` configuration parameter to use local emulation (e.g. `set EventHubsConnection=Memory`).
4. Modify the host.json to choose Netherite as the storage provider:

```json
{
  "version": "2.0",
  "extensions": {
    "durableTask": {
      "storageProvider": {
        "type": "Netherite"
      }
    }
  }
}
```