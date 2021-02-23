#!/usr/bin/pwsh

# read the settings that are common to all scripts
. ./settings.ps1
 
Write-Host "Deleting Resource Group..."
az group delete --name $groupName