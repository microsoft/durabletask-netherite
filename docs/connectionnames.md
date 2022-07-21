
# Connection names

The following two connection names must be defined in app settings or environment variables:

1. `AzureWebJobsStorage` must contain a connection string to an existing Azure Storage account. For most Azure Function applications, this is already the case by default.

?> **Tip** If you want to use a different Azure Storage Account for Netherite task hubs than for the rest of the function app, you can change the `StorageConnectionName` setting in host.json to use a connection name of your choice instead of `AzureWebJobsStorage`.

1. `EventHubsConnection` must contain a SAS connection string to an existing EventHubs namespace.

!> **Important** Never use the same EventHubs namespace for multiple function apps at the same time.


## Connection names for DurableClients

It is possible to bind Durable Clients to specific taskhub instances, as documented [here](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-bindings?tabs=csharp%2C2x-durable-functions#client-usage).
Those APIs would usually expect a single connection name to be specified. However, as Netherite needs two connection names, they can both be passed in the same string, separated by a comma:

<!-- tabs:start -->

#### **C# Attribute**

```csharp
...
[DurableClient(
   TaskHub = "TaskHub2",
   ConnectionName = "Storage2, EventHubs2"
)] IDurableClient client
```

#### **function.json**

```json
{
    "taskHub": "TaskHub2",
    "connectionName": "Storage2, EventHubs2",
}
```

<!-- tabs:end -->
