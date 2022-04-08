# Emulation Mode

For testing or debugging scenarios, we support an emulation mode that eliminates the need for using an actual EventHubs.

Since communication is "simulated in memory", this makes sense only for a single node. Also, it does not guarantee reliable execution.

The emulation mode can be selected by using a "pseudo-connection-string" instead of a real EventHubs connection string:

|String | Interpretation |
|--|--|
|Memory| simulate both the queues and the partition states in memory. |
|MemoryF| simulate the queues in memory, but store the partition states in Azure Storage |

### Quick Summary of Configuration Steps

To configure a Durable Functions application to use the Netherite Emulator:

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
