#!/usr/bin/pwsh
#
# script for running a single scenario n experiment consisting of many orchestrations multiple times, in a deployment
#
param (
    #$GeneratorUrl="http://localhost:7072",
    #$ServiceUrls="http://localhost:7071",    
    $GeneratorUrl="https://sbloadgenerator7924.azurewebsites.net",
    $ServiceUrls="https://functionssb2.azurewebsites.net",
    $Benchmark="all",
    $Target="af"
)

#TODO read these from settings
 
$theDate = (Get-Date).ToUniversalTime().ToString( "yyyy-MM-ddTHH:mm:ss.fffffffZ" )
$testname = "lbfr-$theDate"

function runscenario {

    param ($bm, $t, $op, 
           $no, $robots, 
           $rate, $duration, $warmup, $cooldown, 
           $to)

    if (-not (($Benchmark -eq "all") -or ($bm -eq $Benchmark))) { return }
    if (-not ($Target -eq $t)) { return }
    
    $prefix = "$bm-$t"

    $jsonBase = @{}
    $jsonBase.Add("Prefix",$prefix)
    $jsonBase.Add("Operation",$op)
    $jsonBase.Add("NumberObjects",$no)
    $jsonBase.Add("Robots",$robots)
    $jsonBase.Add("rate",$rate)
    $jsonBase.Add("Duration",$duration)
    $jsonBase.Add("Warmup",$warmup)
    $jsonBase.Add("Cooldown",$cooldown)
    $jsonBase.Add("TimeoutSeconds",$to)
    $jsonBase.Add("ServiceUrls",$ServiceUrls)
    $jsonBase.Add("GeneratorUrl",$GeneratorUrl)
    $Data = ($jsonBase | ConvertTo-Json -Depth 10) | Out-File ".\request.json"

    Write-Host "------------------------------------------------------------------------------------------------"
    Write-Host "Running $prefix testname=$testname"
    Write-Host "on generator $GeneratorUrl for service $ServiceUrls"
    $result = (curl.exe -H "Content-Type: application/json" --max-time 200 "$GeneratorUrl/fixedrate/$testname" -d '@request.json' | ConvertFrom-Json)
    $id = $result.Scenario
    $status = $result.Status
    while ($status -eq "Running")
    {       
        Write-Host $result
        $status = ""
        $result = (curl.exe --max-time 200 "$GeneratorUrl/fixedrate/$testname/$id" | ConvertFrom-Json)
        $status = $result.Status
        Write-Host "Received Response for $prefix : $status"
    }
    if ($status -eq "Completed")
    {
        Write-Host "completed $prefix : " $result.Result.results.summary
        Write-Host "collected: " $result.Result.results.collected "dropped: " $result.Result.results.dropped
     Write-Host "------------------------------------------------------------------------------------------------"
   }
}

# ------ preliminary debugging

#runscenario "wait"         "dbg"  "Wait5"             0 1   1 3 1 1    60
#runscenario "ping"         "dbg"  "Ping"              0 5   1 20 5 5   60
#runscenario "callbacktest" "dbg"  "CallbackTest"      0 1   1 1 0 0    60

# ------ client series

#runscenario "hello-orchestrator" "dbg"    "Hello"              0 1   1 1 0 0   60
#runscenario "hello-orchestrator" "orig"   "Hello"              0 1      1 1 0 0      60
#runscenario "hello-orchestrator" "neth"   "Hello"              0 50   0.2 250 50 0   60

#runscenario "hello-httpclient"   "dbg"    "HelloClient"        0 1   1 1 0 0   60
#runscenario "hello-httpclient"   "orig"   "HelloClient"        0 1      1 1 0 0      60
#runscenario "hello-httpclient"   "neth"   "HelloClient"        0 50   0.2 250 50 0   60

# ------ hello series

runscenario "hello" "aws-dbg"  "HelloAws"    0 1         1 1 0 0      60
runscenario "hello" "aws"      "HelloAws"    0 50     0.2 250 50 0    60

runscenario "hello" "dbg"      "Hello"       0 1         1 3 1 1      60
runscenario "hello" "orig"     "Hello"       0 50     0.2 250 50 0    60
runscenario "hello" "neth-p"   "Hello"       0 50     0.2 250 50 0    60
runscenario "hello" "neth-np"  "Hello"       0 50     0.2 250 50 0    60

runscenario "hello-http" "dbg" "HelloHttp"   0 1         1 3 1 1      60

# ----- bank series

runscenario "bank" "dbg"     "Bank"    1000 1      1 3 1 1      60

runscenario "bank" "orig"    "Bank"    1000 50   0.1 350 50 0   60
runscenario "bank" "neth-p"  "Bank"    1000 50   0.1 350 50 0   60
runscenario "bank" "neth-np" "Bank"    1000 50   0.1 350 50 0   60

# ------ long sequence series

runscenario "seqlong-trig" "aws-dbg" "AwsTriggeredSequenceLong"    10 1     1 1 0 0      100
runscenario "seqlong-trig" "aws"     "AwsTriggeredSequenceLong"    10 100 .01 300 0 0    100

runscenario "seqlong-orch" "dbg"     "OrchestratedSequenceLong"    10 1     1 1 0 0      60
runscenario "seqlong-orch" "orig"    "OrchestratedSequenceLong"    10 100  .1 250 0 0    60
runscenario "seqlong-orch" "neth-p"  "OrchestratedSequenceLong"    10 100  .1 250 0 0    60
runscenario "seqlong-orch" "neth-np" "OrchestratedSequenceLong"    10 100  .1 250 0 0    60

runscenario "seqlong-blob" "dbg"     "TriggeredSequenceLong"       10 10  .005 200 0 0   300
runscenario "seqlong-blob" "af"      "TriggeredSequenceLong"       10 10  .05 3600 0 0   300

runscenario "seqlong-queue" "dbg"    "QueueSequenceLong"           10 5     1 20 0 0     100
runscenario "seqlong-queue" "af"     "QueueSequenceLong"           10 50   .1 300 50 0   100

# ----- image series

runscenario "img" "aws-dbg" "ImageAWS"   0 1     1 1 0 0      60
runscenario "img" "aws"     "ImageAWS"   0 20   .2 350 50 0   60

runscenario "img" "dbg"     "ImageDF"    0 1     1 3 1 1      60
runscenario "img" "orig"    "ImageDF"    0 20   .2 350 50 0   60
runscenario "img" "neth-p"  "ImageDF"    0 20   .2 350 50 0   60
runscenario "img" "neth-np" "ImageDF"    0 20   .2 350 50 0   60

