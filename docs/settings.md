# Netherite Configuration Settings



```c#
{
  "version": "2.0",
  "logging": {
      // ------ The log levels below affect the trace level transmitted or consumed.
      // ------ note that not all consumers respect these settings.
      // - limits what's displayed in the func.exe console
      // - limits what gets stored in application insights
      // - does not limit what is collected by ETW
      // - does not limit what is traced to console
      // - does not limit what is shown in Application Insights Live Metrics side panel
      "DurableTask.Netherite": "Information",
      "DurableTask.Netherite.FasterStorage": "Warning",
      "DurableTask.Netherite.EventHubsTransport": "Warning",
      "DurableTask.Netherite.Events": "Warning",
      "DurableTask.Netherite.WorkItems": "Warning"
    },
  },
  "extensions": {
    "durableTask": {

      // the parameters in this section are the same for all backends,
      // and the same guidance applies.
      "hubName": "perftests",
      "maxConcurrentActivityFunctions": "100",
      "maxConcurrentOrchestratorFunctions": "100",

      // we recommend to always use the following settings for Netherite
      "extendedSessionsEnabled": "true", // important for cache locality
      "UseGracefulShutdown": true, // important to avoid lengthy waits for lease expiration

      "storageProvider": { // the parameters in this section are specific to Netherite

        "StorageConnectionString": "$AzureWebJobsStorage",
        "EventHubsConnectionString": "$EventHubsConnection",
    
        // the following parameters control how often checkpoints are stored
        // more frequent checkpointing means quicker recovery after crashes or ungraceful shutdowns
        // it does not affect reliability (since progress is always persisted in the commit log first anyway)
        "TakeStateCheckpointWhenStoppingPartition": "true",
        "MaxNumberBytesBetweenCheckpoints": "20000000",
        "MaxNumberEventsBetweenCheckpoints": "10000",
        "MaxTimeMsBetweenCheckpoints": "60000",

        // this controls what log information is produced by the various components
        // it limits the *production* of the events (not just transmission or consumption),
        // and thus can be used to prevent overheads
        // even when some consumers (e.g. Application Insights) are configured to collect full trace information
        "LogLevelLimit": "Trace",
        "StorageLogLevelLimit": "Trace",
        "TransportLogLevelLimit": "Trace",
        "EventLogLevelLimit": "Trace",
        "WorkItemLogLevelLimit": "Trace",

        // the following can be used to split and direct trace output to additional specific sinks
        // which is useful in a testing and debugging context
        "TraceToConsole": false,    // all generated trace information is written to Console.Out
        "TraceToBlob": true         // all generated trace information is saved as append blobs in a 'logs' blob container in Azure Storage
      }
    }
  }
}
    
```
