#!/usr/bin/pwsh
param (
	$ResultsFile="./results.csv",
	$PrintColumnNames=$true,
    $RunEP1=$true,
	$RunEP2=$true,
	$RunEP3=$true,
	$DelayAfterDelete=200,
	$tu=20
)

# read the settings that are common to all scripts
. ./settings.ps1

if ($PrintColumnNames)
{
	Add-Content -path $ResultsFile -value "plan,nodes,tag,test,tu,starttime,iteration,size,duration"
}

if ($RunEP1)
{
	./series/runsingle -Tag azst-12    -HubName A11 -Plan EP1 -NumNodes 1 -WaitForDeploy 15 -Orchestration "WordCount?shape=5x20" -Data 5 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runsingle -Tag neth-12-ls -HubName L11 -Plan EP1 -NumNodes 1 -WaitForDeploy 50 -Orchestration "WordCount?shape=10x30" -Data 10 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A12 -Plan EP1 -NumNodes 4 -WaitForDeploy 50 -Orchestration "WordCount?shape=10x30" -Data 10 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L12 -Plan EP1 -NumNodes 4 -WaitForDeploy 80 -Orchestration "WordCount?shape=10x30" -Data 10 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A13 -Plan EP1 -NumNodes 8 -WaitForDeploy 50 -Orchestration "WordCount?shape=10x30" -Data 10 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L13 -Plan EP1 -NumNodes 8 -WaitForDeploy 80 -Orchestration "WordCount?shape=10x30" -Data 10 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A14 -Plan EP1 -NumNodes 12 -WaitForDeploy 50 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L14 -Plan EP1 -NumNodes 12 -WaitForDeploy 80 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP2)
{
	./series/runsingle -Tag azst-12    -HubName A21 -Plan EP2 -NumNodes 1 -WaitForDeploy 15 -Orchestration "WordCount?shape=10x30" -Data 10 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runsingle -Tag neth-12-ls -HubName L21 -Plan EP2 -NumNodes 1 -WaitForDeploy 50 -Orchestration "WordCount?shape=10x30" -Data 10 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A22 -Plan EP2 -NumNodes 4 -WaitForDeploy 50 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L22 -Plan EP2 -NumNodes 4 -WaitForDeploy 80 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A23 -Plan EP2 -NumNodes 8 -WaitForDeploy 50 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L23 -Plan EP2 -NumNodes 8 -WaitForDeploy 80 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A24 -Plan EP2 -NumNodes 12 -WaitForDeploy 50 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L24 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP3)
{
	./series/runsingle -Tag azst-12    -HubName A31 -Plan EP3 -NumNodes 1 -WaitForDeploy 15 -Orchestration "WordCount?shape=10x30" -Data 10 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runsingle -Tag neth-12-ls -HubName L31 -Plan EP3 -NumNodes 1 -WaitForDeploy 50 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A32 -Plan EP3 -NumNodes 4 -WaitForDeploy 50 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L32 -Plan EP3 -NumNodes 4 -WaitForDeploy 80 -Orchestration "WordCount?shape=20x50" -Data 20 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A33 -Plan EP3 -NumNodes 8 -WaitForDeploy 50 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L33 -Plan EP3 -NumNodes 8 -WaitForDeploy 80 -Orchestration "WordCount?shape=20x50" -Data 20 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A34 -Plan EP3 -NumNodes 12 -WaitForDeploy 50 -Orchestration "WordCount?shape=15x40" -Data 15 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L34 -Plan EP3 -NumNodes 12 -WaitForDeploy 80 -Orchestration "WordCount?shape=20x50" -Data 20 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}


