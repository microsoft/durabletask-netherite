#!/usr/bin/pwsh
param (
	$Tag="default",
	$ResultsFile="./results.csv",
    $RunEP1=$true,
	$RunEP2=$true,
	$RunEP3=$true,
	$tu=4,
)

# read the settings that are common to all scripts
. ./settings.ps1

if ($RunEP1)
{
	./series/runseries -Tag azst-12    -HubName A11 -Plan EP1 -NumNodes 1 -WaitForDeploy 15 -Orchestration HelloSequence5 -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 120 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runseries -Tag neth-12-ls -HubName N11 -Plan EP1 -NumNodes 1 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 30 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runseries -Tag azst-12    -HubName A12 -Plan EP1 -NumNodes 4 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 0 -DelayAfterRun 150 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runseries -Tag neth-12-ls -HubName N12 -Plan EP1 -NumNodes 4 -WaitForDeploy 80 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runseries -Tag azst-12    -HubName A13 -Plan EP1 -NumNodes 8 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 0 -DelayAfterRun 150 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runseries -Tag neth-12-ls -HubName N13 -Plan EP1 -NumNodes 8 -WaitForDeploy 80 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runseries -Tag azst-12    -HubName A14 -Plan EP1 -NumNodes 12 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 0 -DelayAfterRun 150 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runseries -Tag neth-12-ls -HubName N14 -Plan EP1 -NumNodes 12 -WaitForDeploy 80 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true
}

if ($RunEP2)
{
	./series/runseries -Tag azst-12    -HubName A21 -Plan EP2 -NumNodes 1 -WaitForDeploy 15 -Orchestration HelloSequence5 -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runseries -Tag neth-12-ls -HubName N21 -Plan EP2 -NumNodes 1 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 20 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runseries -Tag azst-12    -HubName A22 -Plan EP2 -NumNodes 4 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runseries -Tag neth-12-ls -HubName N22 -Plan EP2 -NumNodes 4 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runseries -Tag azst-12    -HubName A23 -Plan EP2 -NumNodes 8 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runseries -Tag neth-12-ls -HubName N23 -Plan EP2 -NumNodes 8 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 50 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runseries -Tag azst-12    -HubName A24 -Plan EP2 -NumNodes 12 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runseries -Tag neth-12-ls -HubName N24 -Plan EP2 -NumNodes 12 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true
}

if ($RunEP3)
{
	./series/runseries -Tag azst-12    -HubName A31 -Plan EP3 -NumNodes 1 -WaitForDeploy 15 -Orchestration HelloSequence5 -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runseries -Tag neth-12-ls -HubName N31 -Plan EP3 -NumNodes 1 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 20 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runseries -Tag azst-12    -HubName A32 -Plan EP3 -NumNodes 4 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runseries -Tag neth-12-ls -HubName N32 -Plan EP3 -NumNodes 4 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 50 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runseries -Tag azst-12    -HubName A33 -Plan EP3 -NumNodes 8 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runseries -Tag neth-12-ls -HubName N33 -Plan EP3 -NumNodes 8 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 40 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runseries -Tag azst-12    -HubName A34 -Plan EP3 -NumNodes 12 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 5000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runseries -Tag neth-12-ls -HubName N34 -Plan EP3 -NumNodes 12 -WaitForDeploy 50 -Orchestration HelloSequence5 -NumOrchestrations 10000 -PortionSize 200 -DelayAfterRun 30 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true
}


