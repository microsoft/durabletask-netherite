#!/usr/bin/pwsh

# In case you are running into issues with Windows execution policies, you can unblock the *.ps1 files via:
# Unblock-File -Path ./*.ps1

Write-Host "Using parameters specified in settings.ps1."

# always edit this parameter before running the scripts
$name = "globally-unique-lowercase-alphanumeric-name-with-no-dashes"

# REVIEW THESE PARAMETERS BEFORE RUNNING THE SCRIPT
$location = "westeurope"
$storageSku = "Standard_LRS"

# optionally, customize the following parameters
# to use different names for resource group, namespace, function app, storage account, and plan
$groupName = $name
$nameSpaceName = $name
$functionAppName = $name
$storageName = $name
$planName = $name

if (($name -eq "globally-unique-lowercase-alphanumeric-name-with-no-dashes")) {
	throw "You must edit the 'name' parameter in settings.ps1 before using this script"
}
