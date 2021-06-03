#!/usr/bin/pwsh
param (
	$ResultsFile="./results.csv",
	$PrintColumnNames=$true,
    $RunEP1=$true,
	$RunEP2=$true,
	$RunEP3=$true,
	$DelayAfterDelete=300,
	$tu=4
)

# read the settings that are common to all scripts
. ./settings.ps1

if ($PrintColumnNames)
{
	Add-Content -path $ResultsFile -value "plan,nodes,tag,test,tu,starttime,iteration,size,duration"
}

if ($RunEP1)
{
	./series/runsingle -Tag azst-12    -HubName A11 -Plan EP1 -NumNodes 1 -WaitForDeploy 120 -Orchestration CollisionSearch/divide-and-conquer -Data 100 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runsingle -Tag neth-12-ls -HubName L11 -Plan EP1 -NumNodes 1 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 100 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A12 -Plan EP1 -NumNodes 4 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 200 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L12 -Plan EP1 -NumNodes 4 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 200 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A13 -Plan EP1 -NumNodes 8 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 300 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L13 -Plan EP1 -NumNodes 8 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 300 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A14 -Plan EP1 -NumNodes 12 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 400 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L14 -Plan EP1 -NumNodes 12 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 400 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP2)
{
 	./series/runsingle -Tag azst-12    -HubName A21 -Plan EP2 -NumNodes 1 -WaitForDeploy 120 -Orchestration CollisionSearch/divide-and-conquer -Data 200 -ResultsFile $ResultsFile -ThroughputUnits 1
  	./series/runsingle -Tag neth-12-ls -HubName L21 -Plan EP2 -NumNodes 1 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 200 -ResultsFile $ResultsFile -ThroughputUnits $tu

 	./series/runsingle -Tag azst-12    -HubName A22 -Plan EP2 -NumNodes 4 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 300 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runsingle -Tag neth-12-ls -HubName L22 -Plan EP2 -NumNodes 4 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 300 -ResultsFile $ResultsFile -ThroughputUnits $tu

    ./series/runsingle -Tag azst-12    -HubName A23 -Plan EP2 -NumNodes 8 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 400 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L23 -Plan EP2 -NumNodes 8 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 400 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A24 -Plan EP2 -NumNodes 12 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 500 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L24 -Plan EP2 -NumNodes 12 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 500 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}

if ($RunEP3)
{
	./series/runsingle -Tag azst-12    -HubName A31 -Plan EP3 -NumNodes 1 -WaitForDeploy 120 -Orchestration CollisionSearch/divide-and-conquer -Data 300 -ResultsFile $ResultsFile -ThroughputUnits 1
 	./series/runsingle -Tag neth-12-ls -HubName L31 -Plan EP3 -NumNodes 1 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 300 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A32 -Plan EP3 -NumNodes 4 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 600 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L32 -Plan EP3 -NumNodes 4 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 600 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A33 -Plan EP3 -NumNodes 8 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 1000 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L33 -Plan EP3 -NumNodes 8 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 1000 -ResultsFile $ResultsFile -ThroughputUnits $tu

	./series/runsingle -Tag azst-12    -HubName A34 -Plan EP3 -NumNodes 12 -WaitForDeploy 50 -Orchestration CollisionSearch/divide-and-conquer -Data 1000 -ResultsFile $ResultsFile -ThroughputUnits 1
	./series/runsingle -Tag neth-12-ls -HubName L34 -Plan EP3 -NumNodes 12 -WaitForDeploy 80 -Orchestration CollisionSearch/divide-and-conquer -Data 1000 -ResultsFile $ResultsFile -ThroughputUnits $tu -DeleteAfterTests $true

	Start-Sleep -Seconds $DelayAfterDelete
}


