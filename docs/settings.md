
## Minimal Configuration

To run with Netherite, the host.json file must set the storage provider type to "Netherite".
```c#
{
  "version": "2.0",
  "extensions": {
    "durableTask": {
      "storageProvider": {
        "type" : "Netherite"
      }
    }
  }
}    
```

And the configuration settings (app settings or environment variables) must specify two connection strings:
1. `AzureWebJobsStorage` must contain a connection string to an existing Azure Storage account. For most Azure Function applications, this is already the case by default.
1. `EventHubsConnection` must contain a SAS connection string to an existing EventHubs namespace. 

## Typical Configuration

A typical host.json configuration file usually contains a few more settings. We recommend something like:

```c#
{
  "version": "2.0",
  "extensions": {
    "durableTask": {

      // The task hub name can be changed to start from a clean state
      "hubName": "myawesomeapp-11",  

      // set this to true to reduce likelihood of duplicated work items   
      "UseGracefulShutdown": "true",

      // tweak these for performance tuning (work the same for all backends)
      "maxConcurrentActivityFunctions": "100",
      "maxConcurrentOrchestratorFunctions": "100",

      "storageProvider": {

        "type" : "Netherite",

        // the number of partitions to use. Cannot be changed after task hub is created.
        "PartitionCount": "12",  

        // where to look for the connection strings
        "StorageConnectionName": "AzureWebJobsStorage",
        "EventHubsConnectionName": "EventHubsConnection"
      }
    }
  }
}    
```

### Partition Count considerations

In the current implementation, the partition count cannot be changed after a task hub is created. Thus, one may want to think briefly about what to choose:

  | PartitionCount| indication |
  |-------|------------|
  | 12 | Performs well across a range of 1-12 nodes. *This is the recommended default*. |
  | 32 | Permits maximal scaleout of up to 32 nodes, but is not recommended for running on a single node with less than 8 cores. |
  | 1  | Achieves optimal performance on a single node. Useful only if there is no intention to *ever* scale out. |

The partition count is similar to the control queue count in the default engine;
however, unlike the latter, it also affects the maximum scale out for activities (not just orchestrations).
For applications that require massive scale for activities, we recommend using a large partition count, and perhaps the use of HttpTriggers in place of activities.

### Tracing and Logging Parameters

There are two different sections that control aspects of the logging.

#### Parameters in the global `logging` section

The `logging` section of the host.json file controls
1. what is displayed in the console (when running func.exe locally), and
2. what is stored and billed in Application Insights (when Application Insights is enabled).

```c#
"version": "2.0",
  "logging": {
    "logLevel": {
       
      // ---- Per-invocation framework-generated logging
      //"Host.Triggers.DurableTask": "Information", // use this setting if you need analytics in the portal
      "Host.Triggers.DurableTask": "Warning", // use this setting otherwise

      // ---- Per-invocation application-generated logging
      //"Function": "Information", // use this setting for small-scale debugging
      "Function": "Warning", // use this setting when running perf tests

      // --- the levels below are used to control the Netherite tracing.
      "DurableTask.Netherite": "Information",
      "DurableTask.Netherite.FasterStorage": "Warning",
      "DurableTask.Netherite.EventHubsTransport": "Warning",
      "DurableTask.Netherite.Events": "Warning",
      "DurableTask.Netherite.WorkItems": "Warning"
    },
  },
  ...
}
```
#### Logging Parameters for Netherite

The generation of tracing events can be controlled in the `durableTask` section. We recommend leaving these at the "Debug" setting which allows useful telemetry to be collected automatically when running in hosted plans. 

```csharp
  "extensions": {
    "durableTask": {
        ...

        // The log level limits below control the production of log events by the various components.
        // it limits production, not just consumption, of the events, so it can be used to prevent overheads.
        // "Debug" is a reasonable setting, as it allows troubleshooting without impacting perf too much.
        "LogLevelLimit": "Debug",
        "StorageLogLevelLimit": "Debug",
        "TransportLogLevelLimit": "Debug",
        "EventLogLevelLimit": "Debug",
        "WorkItemLogLevelLimit": "Debug",

        // the following can be used to collectd and direct trace output to additional specific sinks
        // which is useful in a testing and debugging context, but not recommended for production
        "TraceToConsole": false,
        "TraceToBlob": false
      }
    }
  }
```

### Advanced Netherite-Specific Configuration Parameters

In addition to the parameters shown earlier, there are many more. Many of them are the same as for other backends.
 We document only the ones that are specific to Netherite here.

#### Orchestration Caching

| name | type | meaning| 
| - | - | - |
| CacheOrchestrationCursors | bool | Whether to enable caching of execution cursors to skip orchestrator replay. Defaults to true. |

This setting is the equivalent of `enableExtendedSessions` for the Netherite backend, in the sense that it allows in-progress orchestrations to stay in memory.
However, unlike `enableExtendedSessions`, it is true by default.

#### Other Parameters

| name | type | meaning| 
| - | - | - |
| PackPartitionTaskMessages | int | Maximum number of task messages to pack into a single Event Hubs event. Defaults to 100. |
| LoadInformationAzureTableName | string | a name for an Azure Table to use for publishing load information. Defaults to "DurableTaskPartitions". If set to null or empty, then Azure blobs are used instead.|

#### Checkpointing

The following settings control the frequency of the asynchronous checkpointing. We do not anticipate that
Netherite users need to modify these defaults.

Since progress is already being continously persisted to the commit log, how often to take checkpoints is purely a performance tradeoff (weighing cost of replay versus cost of checkpointing); it does not influence the reliability. 

| name | type | meaning| 
| - | - | - |
| TakeStateCheckpointWhenStoppingPartition |  bool | Whether to checkpoint the current state of a partition when it is stopped. This improves recovery time but lengthens shutdown time.|
| MaxNumberBytesBetweenCheckpoints |  long | A limit on how many bytes to append to the log before initiating a state checkpoint. The default is 20MB.|
| MaxNumberEventsBetweenCheckpoints |  long |A limit on how many events to append to the log before initiating a state checkpoint. The default is 10000. |
| MaxTimeMsBetweenCheckpoints |  long |A limit on how long to wait between state checkpoints, in milliseconds. The default is 60s. |

#### Unsupported Parameters (For Debugging/Experiments Only)

The following settings were used for testing environments or specific experimental investigations. 
Use at your own risk, we do not guarantee that they actually work.

| name |  meaning | 
| - | - |
| PartitionManagementOptions | Selects alternate partition management algorithms.|
| ActivitySchedulerOptions | Selects alternate activity scheduling algorithms. |
| KeepServiceRunning |   Keeps the service running during unit tests. |
| UseLocalDirectoryForPartitionStorage | Save partition states to local files instead of the blob container. |
| UseAlternateObjectStore | Uses Azure Tables for storing partition states instead of FASTER. |
| PersistStepsFirst |  Forces steps to pe persisted before applying their effects, disabling all pipelining. |
| PageBlobStorageConnectionName | Use an alternate storage account specifically to store page blobs. |  
