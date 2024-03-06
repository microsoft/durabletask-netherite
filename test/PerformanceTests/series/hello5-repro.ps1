#!/usr/bin/pwsh
param (
	$ResultsFile="./results.csv",
	$PrintColumnNames=$true,
	$tu=5
)

# read the settings that are common to all scripts
. ./settings.ps1

if ($PrintColumnNames)
{
	Add-Content -path $ResultsFile -value "plan,nodes,tag,test,tu,starttime,iteration,size,duration"
}


./series/runmany -Tag neth -HubName M11 -Plan EP3 -NumNodes 12 -WaitForDeploy 80 -Orchestration HelloSequence5 -NumOrchestrations 10000 -NumReps 8 -PortionSize 200 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu 
