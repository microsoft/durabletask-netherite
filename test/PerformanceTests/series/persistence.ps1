#!/usr/bin/pwsh
param (
	$ResultsFile="./results.csv",
	$PrintColumnNames=$false
)

# read the settings that are common to all scripts
. ./settings.ps1

if ($PrintColumnNames)
{
	Add-Content -path $ResultsFile -value "plan,nodes,tag,test,tu,starttime,iteration,size,duration"
}

./series/runsingle -Tag neth-12-ns -HubName PN3 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 500 -ThroughputUnits 20 
./series/runsingle -Tag neth-12-gs -HubName PG3 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 500 -ThroughputUnits 20
./series/runsingle -Tag neth-12-ls -HubName PL3 -Plan EP2 -NumNodes 12 -WaitForDeploy 100 -Orchestration CollisionSearch/divide-and-conquer -Data 500 -ThroughputUnits 20

./series/runsingle -Tag neth-12-ns -HubName PN0r -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration "WordCount?shape=15x40" -Data 15 -ThroughputUnits 20
./series/runsingle -Tag neth-12-ls -HubName PL0r -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration "WordCount?shape=15x40" -Data 15 -ThroughputUnits 20
./series/runsingle -Tag neth-12-gs -HubName PG0r -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration "WordCount?shape=15x40" -Data 15 -ThroughputUnits 20

./series/runmany -Tag neth-12-ls -HubName PL1 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 30 -ResultsFile $ResultsFile -ThroughputUnits 20
./series/runmany -Tag neth-12-ns -HubName PN1 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 30 -ResultsFile $ResultsFile -ThroughputUnits 20
./series/runmany -Tag neth-12-gs -HubName PG1 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 30 -ResultsFile $ResultsFile -ThroughputUnits 20

./series/runmany -Tag neth-12-ls -HubName PL2 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 50 -ResultsFile $ResultsFile -ThroughputUnits 4
./series/runmany -Tag neth-12-ns -HubName PN2 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 50 -ResultsFile $ResultsFile -ThroughputUnits 4
./series/runmany -Tag neth-12-gs -HubName PG2 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 50 -ResultsFile $ResultsFile -ThroughputUnits 4 -DeleteAfterTests true

