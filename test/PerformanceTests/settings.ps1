#!/usr/bin/pwsh
Write-Host "Using parameters specified in settings.ps1."

# always edit this parameter before running the scripts
$name="functionssb4"

# review these parameters before running the scripts
$location="eastus2"
$storageSku="Standard_LRS"

# optionally, customize the following parameters
# to use different names for resource group, namespace, function app, storage account, and plan
$groupName=$name
$nameSpaceName=$name
$functionAppName=$name
$storageName=$name
$planName=$name

if (($name -eq "globally-unique-lowercase-alphanumeric-name-with-no-dashes")) 
{
	throw "You must edit the 'name' parameter in settings.ps1 before using this script"
}

