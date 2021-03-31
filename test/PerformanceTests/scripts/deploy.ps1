#!/usr/bin/pwsh
param (
	$Plan="EP2", 
	$MinNodes="1", 
	$MaxNodes="20", 
	$Configuration="Release"
	)

# read the settings that are common to all scripts
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

Write-Host "Configuring Scale=$MinNodes-$MaxNodes"
az functionapp plan update -g $groupName -n $functionAppName --max-burst $MaxNodes --number-of-workers $MinNodes --min-instances $MinNodes 
az resource update -n $functionAppName/config/web -g $groupName --set properties.minimumElasticInstanceCount=$MinNodes --resource-type Microsoft.Web/sites
if ($MinNode -eq $MaxNodes)
{
	az resource update -n $functionAppName/config/web -g $groupName --set properties.functionsRuntimeScaleMonitoringEnabled=0 --resource-type Microsoft.Web/sites
}
else
{
	az resource update -n $functionAppName/config/web -g $groupName --set properties.functionsRuntimeScaleMonitoringEnabled=1 --resource-type Microsoft.Web/sites
}

Write-Host "Publishing Code to Function App..."
func azure functionapp publish $functionAppName

Pop-Location
