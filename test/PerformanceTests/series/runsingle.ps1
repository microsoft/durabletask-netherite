#!/usr/bin/pwsh
#
# script for running an experiment consisting of a single orchestration multiple times, in a deployment
#
param (
    $Settings="./settings.ps1",
	$Plan="EP2", 
	$NumNodes="4", 
	$Configuration="Release",
	$ThroughputUnits="1",
	$WaitForDeploy=50,
	$NumReps=6,
	$Orchestration="CollisionSearch/divide-and-conquer",
	$Data="1000",
	$DelayAfterRun = 15,
	$Tag="neth",
	$HubName="perftests",
	$MaxA="",
	$ResultsFile="./results.csv",
	$DeployCode=$true,
	$DeleteAfterTests=$false,
	$PrintColumnNames=$false
	)

if ($PrintColumnNames)
{
    Add-Content -path $ResultsFile -value "plan,nodes,tag,test,tu,starttime,iteration,size,duration"
}

# deploy to a premium plan
. ./scripts/deploy.ps1 -Settings $Settings -Plan $Plan -MinNodes $NumNodes -MaxNodes $NumNodes -Configuration $Configuration -HostConfigurationFile "./series/host.$tag.json" -HubName $HubName -MaxA $MaxA -DeployCode $DeployCode

# update the eventhubs scale
Write-Host "Configuring EventHubs for $ThroughputUnits TU"
az eventhubs namespace update -n $nameSpaceName -g $groupName --capacity $ThroughputUnits

Write-Host "Waiting $WaitForDeploy seconds for deployment to load-balance and start partitions..."
Start-Sleep -Seconds $WaitForDeploy

for($i = 0; $i -lt $NumReps; $i++)
{
	Write-Host "---------- Experiment $i/$NumReps"

	$starttime = (Get-Date).ToUniversalTime().ToString("o")

	Write-Host "Starting $Orchestration -d $Data..."
	$reply = (curl.exe --max-time 300 https://$functionAppName.azurewebsites.net/$Orchestration -d $Data)
	try
	{
		$result = ($reply | ConvertFrom-Json -depth 10)
		Write-Host "RESULT=$result"
	}
	catch
	{
		Write-Host "ERROR: Could not parse reply: $reply"
	}


	Add-Content -path $ResultsFile -value "$Plan,$NumNodes,$Tag,$Orchestration/$Data,$ThroughputUnits,$starttime,$i,$($result.size),$($result.elapsedSeconds)"

    Write-Host "Waiting $DelayAfterRun seconds before continuing..."
	Start-Sleep -Seconds $DelayAfterRun
}

if ($DeleteAfterTests)
{
	Write-Host "Deleting all resources..."
	az group delete --name $groupName --yes
}	