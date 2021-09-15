#!/usr/bin/pwsh
param (
	$ResultsFile="./results.csv",
	$PrintColumnNames=$true,
    $RunEP1=$false,
	$RunEP2=$true,
	$RunEP3=$false,
	$DelayAfterDelete=300,
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
	./series/runmany -Tag azst-12    -HubName A11x -Plan EP1 -NumNodes 1 -WaitForDeploy 120 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 5 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runmany -Tag neth       -HubName L11x -Plan EP1 -NumNodes 1 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 25 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A12x -Plan EP1 -NumNodes 4 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 10 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L12x -Plan EP1 -NumNodes 4 -WaitForDeploy 80 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 50 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A13x -Plan EP1 -NumNodes 8 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 20 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L13x -Plan EP1 -NumNodes 8 -WaitForDeploy 80 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 100 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A14x -Plan EP1 -NumNodes 12 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 40 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L14x -Plan EP1 -NumNodes 12 -WaitForDeploy 80 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 200 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP2)
{
	./series/runmany -Tag azst-12    -HubName A21 -Plan EP2 -NumNodes 1 -WaitForDeploy 120 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 10 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runmany -Tag neth       -HubName L21 -Plan EP2 -NumNodes 1 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 50 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A22 -Plan EP2 -NumNodes 4 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 20 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L22 -Plan EP2 -NumNodes 4 -WaitForDeploy 80 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 100 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A23 -Plan EP2 -NumNodes 8 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 30 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L23 -Plan EP2 -NumNodes 8 -WaitForDeploy 80 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 200 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A24 -Plan EP2 -NumNodes 12 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 50 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L24 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 300 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP3)
{
	./series/runmany -Tag azst-12    -HubName A31 -Plan EP3 -NumNodes 1 -WaitForDeploy 120 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 25 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runmany -Tag neth       -HubName L31 -Plan EP3 -NumNodes 1 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 100 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A32 -Plan EP3 -NumNodes 4 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 50 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L32 -Plan EP3 -NumNodes 4 -WaitForDeploy 80 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 200 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A33 -Plan EP3 -NumNodes 8 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 100 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L33 -Plan EP3 -NumNodes 8 -WaitForDeploy 80 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 400 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A34 -Plan EP3 -NumNodes 12 -WaitForDeploy 50 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 200 -PortionSize 0 -DelayAfterRun 200 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L34 -Plan EP3 -NumNodes 12 -WaitForDeploy 80 -Orchestration FanOutFanInOrchestration -Data "/1000" -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 60 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}


