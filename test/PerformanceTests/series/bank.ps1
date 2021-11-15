#!/usr/bin/pwsh
param (
	$ResultsFile="./bank.csv",
	$PrintColumnNames=$true,
    $RunEP1=$true,
	$RunEP2=$true,
	$RunEP3=$true,
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
	# EP1 apps appear to not be immediately ready for deployment after they are created. So we create it first and then wait a minute.
    . ./scripts/deploy.ps1 -Plan EP1 -MinNodes 1 -MaxNodes 1 -DeployCode $false
	Start-Sleep -Seconds 60
	
	./series/runmany -Tag azst-12    -HubName A11 -Plan EP1 -NumNodes 1 -WaitForDeploy 120 -Orchestration BankTransaction -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 120 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runmany -Tag neth       -HubName L11 -Plan EP1 -NumNodes 1 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 120 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A12 -Plan EP1 -NumNodes 4 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L12 -Plan EP1 -NumNodes 4 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A13 -Plan EP1 -NumNodes 8 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L13 -Plan EP1 -NumNodes 8 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 2000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A14 -Plan EP1 -NumNodes 12 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 1000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L14 -Plan EP1 -NumNodes 12 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 2000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP2)
{
	./series/runmany -Tag azst-12    -HubName A21 -Plan EP2 -NumNodes 1 -WaitForDeploy 120 -Orchestration BankTransaction -NumOrchestrations 2000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runmany -Tag neth       -HubName L21 -Plan EP2 -NumNodes 1 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A22 -Plan EP2 -NumNodes 4 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 2000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L22 -Plan EP2 -NumNodes 4 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A23 -Plan EP2 -NumNodes 8 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 2000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L23 -Plan EP2 -NumNodes 8 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A24 -Plan EP2 -NumNodes 12 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 2000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L24 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP3)
{
	./series/runmany -Tag azst-12    -HubName A31 -Plan EP3 -NumNodes 1 -WaitForDeploy 120 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runmany -Tag neth       -HubName L31 -Plan EP3 -NumNodes 1 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 4000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A32 -Plan EP3 -NumNodes 4 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L32 -Plan EP3 -NumNodes 4 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 4000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A33 -Plan EP3 -NumNodes 8 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L33 -Plan EP3 -NumNodes 8 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 4000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runmany -Tag azst-12    -HubName A34 -Plan EP3 -NumNodes 12 -WaitForDeploy 50 -Orchestration BankTransaction -NumOrchestrations 3000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runmany -Tag neth       -HubName L34 -Plan EP3 -NumNodes 12 -WaitForDeploy 80 -Orchestration BankTransaction -NumOrchestrations 4000 -PortionSize 0 -DelayAfterRun 100 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}


