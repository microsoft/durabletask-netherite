#!/usr/bin/pwsh
param (
	$ResultsFile="./results.csv",
	$PrintColumnNames=$true,
	$tu=20
)

# read the settings that are common to all scripts
. ./settings.ps1

if ($PrintColumnNames)
{
	Add-Content -path $ResultsFile -value "plan,nodes,tag,test,tu,starttime,iteration,size,duration"
}

./series/runsingle -Tag neth -HubName CX0 -Plan Consumption -NumReps 4 -Orchestration FileHash -Data 5000 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runmany -Tag neth   -HubName CX1 -Plan Consumption -NumReps 4 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runmany -Tag neth   -HubName CX2 -Plan Consumption -NumReps 4 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 200 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runmany -Tag neth   -HubName CX3 -Plan Consumption -NumReps 4 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 50 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runsingle -Tag neth -HubName CX4 -Plan Consumption -NumReps 4 -Orchestration WordCount -Data "15x40" -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runsingle -Tag neth -HubName CX5 -Plan Consumption -NumReps 4 -Orchestration CollisionSearch/divide-and-conquer -Data 400 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu
./series/runsingle -Tag neth -HubName CX6 -Plan Consumption -NumReps 4 -Orchestration CollisionSearch/flat-parallel -Data 400 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true
