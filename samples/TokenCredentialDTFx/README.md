# Token Credential Sample

This sample demonstrates how to configure a connection resolver when using Netherite as the orchestration provider for a Durable Task project.

The connection resolver allows you to supply a Azure Token Credential instead of a connection string, which means you can use identity-based authentication.

For this sample, we use the default Azure Identity credential as provided by the Azure.Identity package.

## Configuration Prerequisites

Before running this sample, you must

1. Create a new Azure Storage account, or reuse an existing one
2. Create a new Azure Event Hubs namespace, or reuse an existing one (that is not currently in use)
3. Make sure you have [Storage Blob Data Contributor](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-contributor) and [Storage Table Data Contributor](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-table-data-contributor) permissions for the storage account.
4. Make sure you have [Azure Event Hubs Data Owner](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#azure-event-hubs-data-owner) permission for the event hubs namespace.
5. Set `AccountName` to contain the name of the storage account, using an environment variable or a function app configuration setting,
6. Set `NamespaceName` to contain the name of the Event Hubs namespace, using an environment variable or a function app configuration setting.

