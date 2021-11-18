#!/usr/bin/pwsh

# read the parameters
. ../scripts/settings.ps1

# look up the two connection strings and assign them to the respective environment variables
$Env:AzureWebJobsStorage = (az storage account show-connection-string --name $storageName --resource-group $groupName | ConvertFrom-Json).connectionString
$Env:EventHubsConnection = (az eventhubs namespace authorization-rule keys list --resource-group $groupName --namespace-name $namespaceName --name RootManageSharedAccessKey | ConvertFrom-Json).primaryConnectionString

# install node dependencies
npm install

# install extensions as dependency bundles are not used
func extensions install

# start the function app locally
npm run start