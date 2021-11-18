#!/usr/bin/pwsh

# read the parameters
. ../scripts/settings.ps1

# install pip dependencies
py -m venv .venv

.venv\scripts\activate

pip install -r requirements.txt

# install extensions as dependency bundles are not used
func extensions install

# publish to Azure
Write-Host "Publishing Code to Function App..."
func azure functionapp publish $functionAppName

Pop-Location
