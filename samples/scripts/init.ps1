#!/usr/bin/pwsh

# import the parameter settings from the file in the same directory
. ./settings.ps1
 
if ((az group exists --name $groupName) -ne "true") {
	Write-Host "Creating Resource Group..."
	az group create --name $groupName --location $location
}
else {
	Write-Host "Resource Group already exists."
}

if ((az storage account check-name --name $storageName | ConvertFrom-Json).reason -ne "AlreadyExists") {
	Write-Host "Creating Storage Account..."
	az storage account create --name  $storageName --location $location --resource-group  $groupName --sku $storageSku --allow-blob-public-access false --https-only true --min-tls-version TLS1_2
}
else {
	Write-Host "Storage account already exists."
}
 
if ((az eventhubs namespace exists --name $namespaceName | ConvertFrom-Json).reason -ne "NameInUse") {
	Write-Host "Creating EventHubs Namespace..."
	az eventhubs namespace create --name $namespaceName --resource-group $groupName 
}
else {
	Write-Host "EventHubs Namespace already exists."
}


