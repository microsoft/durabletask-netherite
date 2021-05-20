#!/usr/bin/pwsh
param (
    $Settings="./settings.ps1",
	$Plan="EP2", 
	$NumNodes="4", 
	$Configuration="Release",
	$ThroughputUnits="1",
	$WaitForDeploy=50,
	$NumReps=6,
	$Orchestration="HelloSequence5",
	$NumOrchestrations=10000,
	$PortionSize=200,
	$DelayAfterRun=25,
	$Tag="neth-12-ls",
	$HubName="perftests",
	$ResultsFile="./results.csv",
	$DeployCode=$true,
	$DeleteAfterTests=$false
	)

# deploy to a premium plan
. ./scripts/deploy.ps1 -Settings $Settings -Plan $Plan -MinNodes $NumNodes -MaxNodes $NumNodes -Configuration $Configuration -HostConfigurationFile "./series/host.$tag.json" -HubName $HubName -DeployCode $DeployCode

# update the eventhubs scale
Write-Host "Configuring EventHubs for $ThroughputUnits TU"
az eventhubs namespace update -n $nameSpaceName -g $groupName --capacity $ThroughputUnits

Write-Host "Waiting $WaitForDeploy seconds for deployment to load-balance and start partitions..."
Start-Sleep -Seconds $WaitForDeploy

for($i = 0; $i -lt $NumReps; $i++)
{
	Write-Host "---------- Experiment $i/$NumReps"

	$starttime = (Get-Date).ToUniversalTime().ToString("o")

	if ($StarterEntities -eq 0)
	{
		$arg = $Orchestration + "." + $NumOrchestrations
	}
	else
	{
		$arg = $Orchestration + "." +$NumOrchestrations + "." + $PortionSize
	}

	Write-Host "Starting $arg orchestrations..."
	curl.exe https://$functionAppName.azurewebsites.net/start -d $arg

	Write-Host "Waiting $DelayAfterRun seconds before checking results..."
	Start-Sleep -Seconds $DelayAfterRun

	Write-Host "Checking results..."
	$result = (curl.exe https://$functionAppName.azurewebsites.net/count -d $NumOrchestrations | ConvertFrom-Json)
	Write-Host "RESULT=$result"
	Add-Content -path $ResultsFile -value "$Plan,$NumNodes,$Orchestration.$NumOrchestrations.$PortionSize,$ThroughputUnits,$Tag,$starttime,$($result.completed),$($result.elapsedSeconds)"

	Write-Host "Deleting $NumOrchestrations instances..."
	curl.exe https://$functionAppName.azurewebsites.net/purge -d $NumOrchestrations
}

if ($DeleteAfterTests)
{
	Write-Host "Deleting all resources..."
	az group delete --name $groupName --yes
}