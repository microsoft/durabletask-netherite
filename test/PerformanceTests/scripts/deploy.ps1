#!/usr/bin/pwsh
param (
	$Plan="EP2", 
	$NumNodes="4", 
	$Configuration="Release"
	)

# read the settings
. ./settings.ps1

Write-Host Building $Configuration Configuration...
dotnet build -c $Configuration

# enter the directory with the binaries
Push-Location -Path bin/$Configuration/netcoreapp3.1  

# look up the eventhubs namespace connection string
$eventHubsConnectionString = (az eventhubs namespace authorization-rule keys list --resource-group $groupName --namespace-name $namespaceName --name RootManageSharedAccessKey | ConvertFrom-Json).primaryConnectionString

if (-not ((az functionapp list -g $groupName --query "[].name"| ConvertFrom-Json) -contains $functionAppName))
{
	Write-Host "Creating $Plan Function App..."
	az functionapp plan create --resource-group  $groupName --name  $functionAppName --location $location --sku $Plan
	az functionapp create --name  $functionAppName --storage-account $storageName --plan  $functionAppName --resource-group  $groupName --functions-version 3
    az functionapp config set -n $functionAppName -g $groupName --use-32bit-worker-process false
    az functionapp config appsettings set -n $functionAppName -g  $groupName --settings EventHubsConnection=$eventHubsConnectionString
}
else
{
	Write-Host "Function app already exists."
}

Write-Host "Configuring Scale=$NumNodes..."
az functionapp plan update -g $groupName -n $functionAppName --max-burst $numNodes --number-of-workers $numNodes --min-instances $numNodes 
az resource update -n $functionAppName/config/web -g $groupName --set properties.minimumElasticInstanceCount=$numNodes --resource-type Microsoft.Web/sites

Write-Host "Publishing Code to Function App..."
func azure functionapp publish $functionAppName

Pop-Location
