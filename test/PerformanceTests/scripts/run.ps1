#!/usr/bin/pwsh
param (
    $Settings="./settings.ps1",
	$Configuration="Release"
)

# read the settings and initialize the azure resources
. ./scripts/init.ps1 -Settings $Settings

# build the code
Write-Host Building $Configuration Configuration...
dotnet build -c $Configuration

# enter the directory with the binaries 
Push-Location -Path bin/$Configuration/net6.0  

# look up the two connection strings and assign them to the respective environment variables
$Env:AzureWebJobsStorage = (az storage account show-connection-string --name $storageName --resource-group $groupName | ConvertFrom-Json).connectionString
$Env:EventHubsConnection = (az eventhubs namespace authorization-rule keys list --resource-group $groupName --namespace-name $namespaceName --name RootManageSharedAccessKey | ConvertFrom-Json).primaryConnectionString

# start the function app locally
func start --no-build

Pop-Location
