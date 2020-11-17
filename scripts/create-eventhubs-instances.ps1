Write-Host "Setting parameters..."

# edit these parameters before running the script
$groupName="..."
$nameSpaceName="..."
$location='westus'
$partitionCount=12

if (($groupName -eq "...") -or ($nameSpaceName -eq "...")) 
{
	Write-Error "You have to edit this script: please insert valid parameter values."
	exit
}

Write-Host "Creating Resource Group..."
az group create --name $groupName  --location $location

Write-Host "Creating EventHubs Namespace..."
az eventhubs namespace create --name $nameSpaceName --resource-group $groupName 

Write-Host "Creating EventHubs partitions..."
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name partitions --message-retention 1 --partition-count $partitionCount
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients0 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients1 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients2 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients3 --message-retention 1 --partition-count 32

Write-Host "Done."
