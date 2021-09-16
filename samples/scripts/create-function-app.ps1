#!/usr/bin/pwsh

# review these parameters before running the script
param (
	$Plan = "EP1", 
	$MinNodes = "1", 
	$MaxNodes = "20", 
	$Runtime = "python",
	$RuntimeVersion = "3.8",
	$OsType = "Linux"
)

# read the parameters
. ./settings.ps1

# look up the eventhubs namespace connection string
$eventHubsConnectionString = (az eventhubs namespace authorization-rule keys list --resource-group $groupName --namespace-name $namespaceName --name RootManageSharedAccessKey | ConvertFrom-Json).primaryConnectionString

if (-not ((az functionapp list -g $groupName --query "[].name" | ConvertFrom-Json) -contains $functionAppName)) {
	Write-Host "Creating $Plan Function App..."
	
	if ($OsType -eq "Windows") {
		Write-Host "plan in Windows variant..."
		az functionapp plan create --resource-group  $groupName --name  $functionAppName --location $location --sku $Plan
	}
	else {
		Write-Host "plan in Linux variant..."
		az functionapp plan create --resource-group  $groupName --name  $functionAppName --location $location --sku $Plan --is-linux true
	}
	

	if ($Runtime -eq "dotnet") {
		Write-Host "app for .NET"
		az functionapp create --name  $functionAppName --storage-account $storageName --plan  $functionAppName --resource-group  $groupName --functions-version 3 --runtime $Runtime --os-type $OsType
	}
	else {
		Write-Host "app for $Runtime"
		az functionapp create --name  $functionAppName --storage-account $storageName --plan  $functionAppName --resource-group  $groupName --functions-version 3 --runtime $Runtime --runtime-version $RuntimeVersion --os-type $OsType		
	}
	
	az functionapp config set -n $functionAppName -g $groupName --use-32bit-worker-process false
	az functionapp config appsettings set -n $functionAppName -g  $groupName --settings EventHubsConnection=$eventHubsConnectionString
}
else {
	Write-Host "Function app already exists."
}

Write-Host "Configuring Scale=$MinNodes-$MaxNodes"
az functionapp plan update -g $groupName -n $functionAppName --max-burst $MaxNodes --number-of-workers $MinNodes --min-instances $MinNodes 
az resource update -n $functionAppName/config/web -g $groupName --set properties.minimumElasticInstanceCount=$MinNodes --resource-type Microsoft.Web/sites
if ($MinNode -eq $MaxNodes) {
	az resource update -n $functionAppName/config/web -g $groupName --set properties.functionsRuntimeScaleMonitoringEnabled=0 --resource-type Microsoft.Web/sites
}
else {
	az resource update -n $functionAppName/config/web -g $groupName --set properties.functionsRuntimeScaleMonitoringEnabled=1 --resource-type Microsoft.Web/sites
}
