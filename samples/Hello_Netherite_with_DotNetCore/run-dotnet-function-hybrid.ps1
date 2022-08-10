#!/usr/bin/pwsh

# read the parameters
. ../scripts/settings.ps1

# Build app
dotnet build

# enter the directory with the debug binaries
if (-not (Test-Path -Path ./bin/Debug/net6.0/bin)) {
    throw 'No debug binaries found. Must `dotnet build` first.'
} else {
    cd bin/Debug/net6.0
}

# look up the two connection strings and assign them to the respective environment variables
$Env:AzureWebJobsStorage = (az storage account show-connection-string --name $storageName --resource-group $groupName | ConvertFrom-Json).connectionString
$Env:EventHubsConnection = (az eventhubs namespace authorization-rule keys list --resource-group $groupName --namespace-name $namespaceName --name RootManageSharedAccessKey | ConvertFrom-Json).primaryConnectionString

# start the function app locally
func start --no-build