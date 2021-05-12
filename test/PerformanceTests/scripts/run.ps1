#!/usr/bin/pwsh
param (
	$Configuration="Release"
	)

# initialize the settings and create Azure resources if necessary
. ./scripts/init.ps1

# enter the directory with the binaries
if (-not (Test-Path -Path ./bin/$Configuration/netcoreapp3.1/bin)) {
    throw "No $Configuration binaries found. Must `dotnet build -c $Configuration` first."
} else {
	Push-Location -Path bin/$Configuration/netcoreapp3.1  
}

# look up the two connection strings and assign them to the respective environment variables
$Env:AzureWebJobsStorage = (az storage account show-connection-string --name $storageName --resource-group $groupName | ConvertFrom-Json).connectionString
$Env:EventHubsConnection = (az eventhubs namespace authorization-rule keys list --resource-group $groupName --namespace-name $namespaceName --name RootManageSharedAccessKey | ConvertFrom-Json).primaryConnectionString

# start the function app locally
func start --no-build

Pop-Location
