#!/usr/bin/pwsh
param (
    $Settings="./settings.ps1",
    $Configuration="Release"
)

# read the settings that are common to all scripts
. $Settings
 
# look up the two connection strings and assign them to the respective environment variables
$Env:AzureWebJobsStorage = (az storage account show-connection-string --name $storageName --resource-group $groupName | ConvertFrom-Json).connectionString
$Env:EventHubsConnection = (az eventhubs namespace authorization-rule keys list --resource-group $groupName --namespace-name $namespaceName --name RootManageSharedAccessKey | ConvertFrom-Json).primaryConnectionString

# enter the directory with the binaries 
Push-Location -Path bin/$Configuration/net6.0  

# start an interactive windows cmd shell in the deployment directory
cmd

# when exiting the shell, restore the old directory
Pop-Location
