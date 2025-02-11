#!/usr/bin/pwsh
param (
    $Settings="./settings.ps1",
	$Plan="EP2", 
	$MinNodes="1", 
	$MaxNodes="20", 
	$Configuration="Release",
	$HostConfigurationFile="./host.json",
	$HubName="",
	$MaxA="",
	$DeployCode=$true
)

# read the settings and initialize the azure resources
. ./scripts/init.ps1 -Settings $Settings

if ($DeployCode)
{
    Write-Host Building $Configuration Configuration...
    dotnet build -c $Configuration
	$hostconf = (Get-Content $HostConfigurationFile | ConvertFrom-Json -Depth 32)

	if (-not ($HubName -eq ""))
	{
	    $hostconf.extensions.durableTask.hubName = $HubName
	}
	if (-not ($MaxA -eq ""))
	{
	    $hostconf.extensions.durableTask | Add-Member -Force -NotePropertyName "maxConcurrentActivityFunctions" -NotePropertyValue $MaxA
	}
	if (-not ($MaxO -eq ""))
	{
	    $hostconf.extensions.durableTask | Add-Member -Force -NotePropertyName "maxConcurrentOrchestratorFunctions" -NotePropertyValue $MaxO
	}

	$hostconf | ConvertTo-Json -depth 32 | set-content "./bin/$Configuration/net6.0/host.json"
}

if (-not ((az functionapp list -g $groupName --query "[].name"| ConvertFrom-Json) -contains $functionAppName))
{
	# look up connection strings
	$eventHubsConnectionString = (az eventhubs namespace authorization-rule keys list --resource-group $groupName --namespace-name $namespaceName --name RootManageSharedAccessKey | ConvertFrom-Json).primaryConnectionString
    $corpusConnectionString = (az storage account show-connection-string --name gutenbergcorpus --resource-group corpus | ConvertFrom-Json).connectionString

	Write-Host "Creating $Plan Function App..."

    if ($Plan -eq "Consumption")
    {
        az functionapp create --name  $functionAppName --storage-account $storageName --consumption-plan-location $location --resource-group  $groupName --functions-version 4
    }
    else
    {
        az functionapp plan create --resource-group  $groupName --name  $functionAppName --location $location --sku $Plan
        az functionapp create --name  $functionAppName --storage-account $storageName --plan  $functionAppName --resource-group  $groupName --functions-version 4
    }

	az functionapp config set -n $functionAppName -g $groupName --use-32bit-worker-process false
    az functionapp config appsettings set -n $functionAppName -g  $groupName --settings EventHubsConnection=$eventHubsConnectionString
    az functionapp config appsettings set -n $functionAppName -g  $groupName --settings EHNamespace=$Env:EHNamespace
}
else
{
	Write-Host "Function app already exists."
}


if (-not ($Plan -eq "Consumption"))
{
	Write-Host "Configuring Function App Scale=$MinNodes-$MaxNodes"
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
}

if ($DeployCode)
{
	# enter the directory with the binaries
	Push-Location -Path bin/$Configuration/net6.0  

	Write-Host "Publishing Code to Function App..."
	func azure functionapp publish $functionAppName

	#restore the original directory
	Pop-Location
}
