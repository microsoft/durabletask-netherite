#!/usr/bin/pwsh
param (
    $Settings="./settings.ps1"
)

# read the settings that are common to all scripts
. $Settings
 
# look up the two connection strings and assign them to the respective environment variables
$Env:AzureWebJobsStorage = (az storage account show-connection-string --name $storageName --resource-group $groupName | ConvertFrom-Json).connectionString
$Env:EventHubsConnection = (az eventhubs namespace authorization-rule keys list --resource-group $groupName --namespace-name $namespaceName --name RootManageSharedAccessKey | ConvertFrom-Json).primaryConnectionString

# open visual studio
devenv ..\..\DurableTask.Netherite.sln /noscale