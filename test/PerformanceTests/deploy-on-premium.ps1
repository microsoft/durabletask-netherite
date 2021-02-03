#!/usr/bin/pwsh

# review these parameters before running the script
$numNodes=12
$planSku="EP2"

# read the parameters
. ./settings.ps1

# enter the directory with the release binaries
if (-not (Test-Path -Path ./bin/Release/netcoreapp3.1/bin)) {
    throw 'No release binaries found. Must `dotnet build -c Release` first.'
} else {
    cd bin/Release/netcoreapp3.1
}

# look up the eventhubs namespace connection string
$eventHubsConnectionString = (az eventhubs namespace authorization-rule keys list --resource-group $groupName --namespace-name $namespaceName --name RootManageSharedAccessKey | ConvertFrom-Json).primaryConnectionString

if ((az functionapp show --resource-group $groupName --name $functionAppName | ConvertFrom-Json).name -ne $functionAppName)
{
	Write-Host "Creating Function App..."
	az functionapp plan create --resource-group  $groupName --name  $functionAppName --location $location --sku $planSku
	az functionapp create --name  $functionAppName --storage-account $storageName --plan  $functionAppName --resource-group  $groupName --functions-version 3
    az functionapp config set -n $functionAppName -g $groupName --use-32bit-worker-process false
    az functionapp config appsettings set -n $functionAppName -g  $groupName --settings EventHubsConnection=$eventHubsConnectionString
}
else
{
	Write-Host "Function app already exists."
}

Write-Host "Configuring Scale..."
az functionapp plan update -g  $groupName -n  $functionAppName --max-burst $numNodes --number-of-workers $numNodes --min-instances $numNodes 
az resource update -n  $functionAppName/config/web  -g  $groupName --set properties.minimumElasticInstanceCount=$numNodes --resource-type Microsoft.Web/sites

Write-Host "Publishing Code to Function App..."
func azure functionapp publish $functionAppName
