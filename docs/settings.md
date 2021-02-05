# Netherite Configuration Settings

## Recommended Minimal Configuration

We recommend to always tweak the default settings in the host.json file, because Netherite works a bit different than the other engines.

A minimal host.json file would look like this:

```c#
{
  "version": "2.0",
  "extensions": {
    "durableTask": {

      // we recommend to specify a descriptive name for the taskhub
      "hubName": "myawesomeapp",  

      // we recommend to always use the following settings for Netherite      
      "extendedSessionsEnabled": "true",   // important for cache locality
      "UseGracefulShutdown": "true",       // important to avoid lengthy waits for lease expiration

      "storageProvider": {

        // the number of partitions to use
        "PartitionCount": "12",  

        // where to find the connection strings
        "StorageConnectionName": "AzureWebJobsStorage",
        "EventHubsConnectionName": "EventHubsConnection"

      }
    }
  }
}    
```

### Partition Count considerations

In the current implementation, the partition count for an existing TaskHub cannot be changed. Thus, one may want to think briefly about what to choose:

  | PartitionCount| indication |
  |-------|------------|
  | 12 | Performs well across a range of 1-12 nodes. *This is the recommended default*. |
  | 32 | Permits maximal scaleout of up to 32 nodes, but is not recommended for running on a single node with less than 8 cores. |
  | 1  | Achieves optimal performance on a single node. Useful only if there is no intention to *ever* scale out. |

The partition count is similar to the control queue count in the default engine;
however, unlike the latter, it also affects the maximum scale out for activities (not just orchestrations).
For applications that require massive scale for activities, we recommend using a large partition count, and perhaps the use of HttpTriggers in place of activities.


### Advanced Parameters

In addition to the parameters shown earlier, there are many more that can be tweaked for more advanced scenarios. Here is a sample host.json file:

```c#
{
  "version": "2.0",
  "logging": {
    "logLevel": {
      "default": "Warning",

      // ------ The log levels below affect some (but not all) consumers
      // - limits what's displayed in the func.exe console
      // - limits what gets stored in application insights
      // - does not limit what is collected by ETW
      // - does not limit what is traced to console
      // - does not limit what is shown in Live Metrics side panel
      "DurableTask.Netherite": "Information",
      "DurableTask.Netherite.FasterStorage": "Warning",
      "DurableTask.Netherite.EventHubsTransport": "Warning",
      "DurableTask.Netherite.Events": "Warning",
      "DurableTask.Netherite.WorkItems": "Warning"

    }
  },
  "extensions": {    
    "durableTask": {
    
      // we recommend to specify a descriptive name for the taskhub
      "hubName": "myawesomeapp",  

      // we recommend to always use the following settings for Netherite
      "extendedSessionsEnabled": "true", // important for cache locality
      "UseGracefulShutdown": true, // important to avoid lengthy waits for lease expiration
     
      // these are the same for all backends, the same guidance applies.
      "maxConcurrentActivityFunctions": "100",
      "maxConcurrentOrchestratorFunctions": "100",
     
      "storageProvider": { // the parameters in this section are specific to Netherite

        "StorageConnectionName": "AzureWebJobsStorage",   // where to find the connection string for the storage account
        "EventHubsConnectionName": "EventHubsConnection"  // where to find the connection string for the eventhubs namespace

        // the following parameters control how often checkpoints are stored
        // more frequent checkpointing means quicker recovery after crashes or ungraceful shutdowns
        // it does not affect reliability (since progress is always persisted in the commit log first anyway)
        "TakeStateCheckpointWhenStoppingPartition": "true",
        "MaxNumberBytesBetweenCheckpoints": "20000000",
        "MaxNumberEventsBetweenCheckpoints": "10000",
        "MaxTimeMsBetweenCheckpoints": "60000",

        // how many partitions to create (takes effect only if the taskhub does not already exist)
        "PartitionCount": 12,

        // this controls what log information is produced by the various components
        // it limits production of the events, and thus can be used to prevent overheads
        // even when some consumers (e.g. Application Insights) are configured to trace information at the lowest level
        "LogLevelLimit": "Information",
        "StorageLogLevelLimit": "Information",
        "TransportLogLevelLimit": "Information",
        "EventLogLevelLimit": "Information",
        "WorkItemLogLevelLimit": "Information",

        // the following can be used to split and direct trace output to additional specific sinks
        // which is useful in a testing and debugging context
        "TraceToConsole": false,
        "TraceToBlob": false
      }
    }
  }
}
```