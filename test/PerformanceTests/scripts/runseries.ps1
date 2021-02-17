#!/usr/bin/pwsh
param (
	$Plan="EP1", 
	$NumNodes="4", 
	$Configuration="Release",
	$WaitForDeploy=60,
	$NumReps=6,
	$Orchestration="HelloSequence",
	$NumOrchestrations=1000,
	$StarterEntities=0,
	$DelayAfterRun=10
	)

# import the parameter settings from the file in the same directory
. ./settings.ps1

# create the resources
./scripts/init.ps1
 
# deploy to a premium plan
./scripts/deploy.ps1 -Plan $Plan -NumNodes $NumNodes -Configuration $Configuration

Write-Host "Waiting $WaitForDeploy seconds for deployment to load-balance and start partitions..."
Start-Sleep -Seconds $WaitForDeploy

for($i = 0; $i -lt $NumReps; $i++)
{
	Write-Host "---------- Experiment $i/$NumReps"

	if ($StarterEntities -eq 0)
	{
		$arg = $Orchestration + "." + $NumOrchestrations
	}
	else
	{
		$arg = $Orchestration + "." +$NumOrchestrations + "." + $StarterEntities
	}

	Write-Host "Starting $arg orchestrations..."
	curl.exe https://$functionAppName.azurewebsites.net/start -d $arg

	Write-Host "Waiting $DelayAfterRun seconds before checking results..."
	Start-Sleep -Seconds $DelayAfterRun

	Write-Host "Checking results..."
	curl.exe https://$functionAppName.azurewebsites.net/count -d $NumOrchestrations

	Write-Host "Deleting $NumOrchestrations instances..."
	curl.exe https://$functionAppName.azurewebsites.net/purge -d $NumOrchestrations
}

Write-Host "Deleting all resources..."
az group delete --name $groupName --yes