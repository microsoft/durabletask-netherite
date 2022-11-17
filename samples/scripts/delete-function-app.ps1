#!/usr/bin/pwsh
 
# read the generic parameters
. ./settings.ps1

Write-Host "Deleting Function App..."
az functionapp delete --resource-group $groupName	--name $functionAppName 

Write-Host "Deleting Function Plan..."
az functionapp plan delete --resource-group $groupName --name $functionAppName  --yes

