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
	./series/runmany -Tag azst-12    -HubName A11 -Plan EP1 -NumNodes 1 -WaitForDeploy 15 -Orchestration Bank -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runmany -Tag neth-12-ls -HubName L11 -Plan EP1 -NumNodes 1 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A12 -Plan EP1 -NumNodes 4 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth-12-ls -HubName L12 -Plan EP1 -NumNodes 4 -WaitForDeploy 80 -Orchestration Bank -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A13 -Plan EP1 -NumNodes 8 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth-12-ls -HubName L13 -Plan EP1 -NumNodes 8 -WaitForDeploy 80 -Orchestration Bank -NumOrchestrations 2000 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A14 -Plan EP1 -NumNodes 12 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth-12-ls -HubName L14 -Plan EP1 -NumNodes 12 -WaitForDeploy 80 -Orchestration Bank -NumOrchestrations 2000 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP2)
{
	./series/runmany -Tag azst-12    -HubName A21 -Plan EP2 -NumNodes 1 -WaitForDeploy 15 -Orchestration Bank -NumOrchestrations 2000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runmany -Tag neth-12-ls -HubName L21 -Plan EP2 -NumNodes 1 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A22 -Plan EP2 -NumNodes 4 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 2000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth-12-ls -HubName L22 -Plan EP2 -NumNodes 4 -WaitForDeploy 80 -Orchestration Bank -NumOrchestrations 3000 -PortionSize 200 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A23 -Plan EP2 -NumNodes 8 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 2000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth-12-ls -HubName L23 -Plan EP2 -NumNodes 8 -WaitForDeploy 80 -Orchestration Bank -NumOrchestrations 3000 -PortionSize 200 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A24 -Plan EP2 -NumNodes 12 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 2000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth-12-ls -HubName L24 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration Bank -NumOrchestrations 3000 -PortionSize 200 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP3)
{
	./series/runmany -Tag azst-12    -HubName A31 -Plan EP3 -NumNodes 1 -WaitForDeploy 15 -Orchestration Bank -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runmany -Tag neth-12-ls -HubName L31 -Plan EP3 -NumNodes 1 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 4000 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A32 -Plan EP3 -NumNodes 4 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 3000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth-12-ls -HubName L32 -Plan EP3 -NumNodes 4 -WaitForDeploy 80 -Orchestration Bank -NumOrchestrations 4000 -PortionSize 200 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A33 -Plan EP3 -NumNodes 8 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 3000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth-12-ls -HubName L33 -Plan EP3 -NumNodes 8 -WaitForDeploy 80 -Orchestration Bank -NumOrchestrations 4000 -PortionSize 200 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A34 -Plan EP3 -NumNodes 12 -WaitForDeploy 50 -Orchestration Bank -NumOrchestrations 3000 -PortionSize 200 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth-12-ls -HubName L34 -Plan EP3 -NumNodes 12 -WaitForDeploy 80 -Orchestration Bank -NumOrchestrations 4000 -PortionSize 200 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}


