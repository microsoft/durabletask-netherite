#!/usr/bin/pwsh

# REVIEW THESE PARAMETERS BEFORE RUNNING THE SCRIPT
param (
	$Plan = "EP1", 
	$MinNodes = "1", 
	$MaxNodes = "20", 
	$Runtime = "node",
	$RuntimeVersion = "14",
	$OsType = "Windows"
)

# call the generic script
. ./create-function-app.ps1

