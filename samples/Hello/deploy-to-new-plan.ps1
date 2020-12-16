Write-Host "Setting parameters..."

# edit these parameters before running the script
$name="unique-alphanumeric-name-no-dashes"
$location="westus"
$storageSku="Standard_LRS"
$planSku="EP2"
$numNodes=1

if (($name -eq "unique-alphanumeric-name-no-dashes")) 
{
	Write-Error "You have to edit this script: please insert valid parameter values."
	exit
}
if (($Env:EventHubsConnection -eq $null) -or ($Env:EventHubsConnection -eq "")) 
{
	Write-Error "You have to set the environment variable EventHubsConnection to use this script."
	exit
}

# by default, use the same name for group, function app, storage account, and plan
$groupName=$name
$functionAppName=$name
$storageName=$name
$planName=$name

Write-Host "Creating Resource Group..."
az group create --name $groupName --location $location

Write-Host "Creating Storage Account..."
az storage account create --name  $storageName --location $location --resource-group  $groupName --sku $storageSku

Write-Host "Creating and Configuring Function App..."
az functionapp plan create --resource-group  $groupName --name  $name --location $location --sku $planSku
az functionapp create --name  $functionAppName --storage-account  $storageName --plan  $functionAppName --resource-group  $groupName --functions-version 3
az functionapp plan update -g  $groupName -n  $functionAppName --max-burst $numNodes --number-of-workers $numNodes --min-instances $numNodes 
az resource update -n  $functionAppName/config/web  -g  $groupName --set properties.minimumElasticInstanceCount=$numNodes --resource-type Microsoft.Web/sites
az functionapp config appsettings set -n $functionAppName -g  $groupName --settings EventHubsConnection=$Env:EventHubsConnection
az functionapp config set -n $functionAppName -g $groupName --use-32bit-worker-process false

Write-Host "Publishing Code to Function App..."
func azure functionapp publish $functionAppName
