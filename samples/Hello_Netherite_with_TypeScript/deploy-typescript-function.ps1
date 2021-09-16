#!/usr/bin/pwsh

# read the parameters
. ../scripts/settings.ps1

npm install

npm run build

npm prune --production

func extensions install

Write-Host "Publishing Code to Function App..."
func azure functionapp publish $functionAppName

Pop-Location