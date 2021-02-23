#!/usr/bin/pwsh

# read the settings that are common to all scripts
. ./settings.ps1
 
# look up the two connection strings and assign them to the respective environment variables
$Env:AzureWebJobsStorage = (az storage account show-connection-string --name $storageName --resource-group $groupName | ConvertFrom-Json).connectionString
$Env:EventHubsConnection = (az eventhubs namespace authorization-rule keys list --resource-group $groupName --namespace-name $namespaceName --name RootManageSharedAccessKey | ConvertFrom-Json).primaryConnectionString

# open visual studio
devenv ..\..\DurableTask.Netherite.sln