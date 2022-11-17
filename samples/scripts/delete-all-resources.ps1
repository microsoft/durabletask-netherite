#!/usr/bin/pwsh

# import the parameter settings from the file in the same directory
. ./settings.ps1
 
Write-Host "Deleting Resource Group..."
az group delete --name $groupName --yes
