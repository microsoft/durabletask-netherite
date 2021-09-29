#!/usr/bin/pwsh
param (
	$ResultsFile="./wordcount.csv",
	$PrintColumnNames=$true,
    $RunEP1=$false,
	$RunEP2=$true,
	$RunEP3=$false,
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
    # EP1 apps appear to not be immediately ready for deployment after they are created. So we create it first and then wait a minute.
    #. ./scripts/deploy.ps1 -Plan EP1 -MinNodes 1 -MaxNodes 1 -DeployCode $false
	#Start-Sleep -Seconds 60

	./series/runsingle -Tag azst-12  -HubName A11 -Plan EP1 -NumNodes 1 -WaitForDeploy 15 -Orchestration "WordCount" -Data "5x10" -ResultsFile $ResultsFile -DelayAfterRun 20 -ThroughputUnits 1
	./series/runsingle -Tag neth     -HubName L11 -Plan EP1 -NumNodes 1 -WaitForDeploy 50 -Orchestration "WordCount" -Data "10x10" -ResultsFile $ResultsFile -DelayAfterRun 60 -ThroughputUnits $tu

	./series/runsingle -Tag azst-12  -HubName A12 -Plan EP1 -NumNodes 4 -WaitForDeploy 50 -Orchestration "WordCount" -Data "10x10" -ResultsFile $ResultsFile -DelayAfterRun 20 -ThroughputUnits 1
	./series/runsingle -Tag neth     -HubName L12 -Plan EP1 -NumNodes 4 -WaitForDeploy 80 -Orchestration "WordCount" -Data "20x10" -ResultsFile $ResultsFile -DelayAfterRun 60 -ThroughputUnits $tu

	./series/runsingle -Tag azst-12 -HubName A13 -Plan EP1 -NumNodes 8 -WaitForDeploy 50 -Orchestration "WordCount" -Data "15x20" -ResultsFile $ResultsFile -DelayAfterRun 20 -ThroughputUnits 1
	./series/runsingle -Tag neth    -HubName L13 -Plan EP1 -NumNodes 8 -WaitForDeploy 80 -Orchestration "WordCount" -Data "30x20" -ResultsFile $ResultsFile -DelayAfterRun 60 -ThroughputUnits $tu

	./series/runsingle -Tag azst-12 -HubName A14 -Plan EP1 -NumNodes 12 -WaitForDeploy 50 -Orchestration "WordCount" -Data "20x20" -ResultsFile $ResultsFile -DelayAfterRun 20 -ThroughputUnits 1
	./series/runsingle -Tag neth    -HubName L14 -Plan EP1 -NumNodes 12 -WaitForDeploy 80 -Orchestration "WordCount" -Data "40x20" -ResultsFile $ResultsFile -DelayAfterRun 60 -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP2)
{
	./series/runsingle -Tag azst-12    -HubName A21 -Plan EP2 -NumNodes 1 -WaitForDeploy 15 -Orchestration "WordCount" -Data "10x10" -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runsingle -Tag neth       -HubName L21 -Plan EP2 -NumNodes 1 -WaitForDeploy 50 -Orchestration "WordCount" -Data "20x10" -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A22 -Plan EP2 -NumNodes 4 -WaitForDeploy 50 -Orchestration "WordCount" -Data "15x10" -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth       -HubName L22 -Plan EP2 -NumNodes 4 -WaitForDeploy 80 -Orchestration "WordCount" -Data "30x20" -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A23 -Plan EP2 -NumNodes 8 -WaitForDeploy 50 -Orchestration "WordCount" -Data "20x20" -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth       -HubName L23 -Plan EP2 -NumNodes 8 -WaitForDeploy 80 -Orchestration "WordCount" -Data "40x30" -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A24 -Plan EP2 -NumNodes 12 -WaitForDeploy 50 -Orchestration "WordCount" -Data "25x20" -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth       -HubName L24 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration "WordCount" -Data "50x40" -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP3)
{
	./series/runsingle -Tag azst-12    -HubName A31 -Plan EP3 -NumNodes 1 -WaitForDeploy 15 -Orchestration "WordCount" -Data "10x10" -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runsingle -Tag neth       -HubName L31 -Plan EP3 -NumNodes 1 -WaitForDeploy 50 -Orchestration "WordCount" -Data "20x10" -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A32 -Plan EP3 -NumNodes 4 -WaitForDeploy 50 -Orchestration "WordCount" -Data "15x10" -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth       -HubName L32 -Plan EP3 -NumNodes 4 -WaitForDeploy 80 -Orchestration "WordCount" -Data "40x20" -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A33 -Plan EP3 -NumNodes 8 -WaitForDeploy 50 -Orchestration "WordCount" -Data "25x20" -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth       -HubName L33 -Plan EP3 -NumNodes 8 -WaitForDeploy 80 -Orchestration "WordCount" -Data "60x30" -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A34 -Plan EP3 -NumNodes 12 -WaitForDeploy 50 -Orchestration "WordCount" -Data "30x25" -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth       -HubName L34 -Plan EP3 -NumNodes 12 -WaitForDeploy 80 -Orchestration "WordCount" -Data "80x36" -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}


