#!/usr/bin/pwsh

# review these parameters before running the script
$Configuration = "Release"

# read the parameters
. ../scripts/settings.ps1

Write-Host "Building Function App..."

Write-Host Building $Configuration Configuration...
dotnet build -c $Configuration

# enter the directory with the binaries
Push-Location -Path bin/$Configuration/netcoreapp3.1  


Write-Host "Publishing Code to Function App..."
func azure functionapp publish $functionAppName

Pop-Location
