#!/usr/bin/pwsh
param (
    $Settings="./settings.ps1"
)

# read the settings that are common to all scripts
. $Settings
 
Write-Host "Deleting Resource Group..."
az group delete --name $groupName