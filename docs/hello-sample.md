# Running the 'Hello' Sample

Below we include simple instructions for running the 'Hello' sample project locally and in the cloud. We assume basic familiarity with how to develop and run a DF application in C#, and with how to navigate the Azure Portal. To learn more about DF, see this [tutorial](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-create-first-csharp?WT.mc_id=email), for example.

## Locally, Emulated

If you have the Azure Storage Emulator installed, you can run the sample locally, in "emulation mode". To do so, set the following environment variables before building and running `samples/Hello/Hello.csproj`:

```powershell
set AzureWebJobsStorage=UseDevelopmentStorage=true;
set EventHubsConnection=MemoryF:12
```
If you don't have the Azure Storage Emulator, you will need to use an Azure subscription and Azure Storage account. Just replace `UseDevelopmentStorage=true;` with the connection string to the Azure Storage account.

Note that if working with Visual Studio, you can just hit the run button since the Hello sample is set as the startup project by default. You may have to restart VS if you changed the environment variables after opening VS.

Once the Functions runtime has started successfully, you can run the orchestration by issuing an http request:

```shell
curl http://localhost:7071/api/hellocities
```

## Locally, with EventHubs

To use a real EventHubs, you first have to create an EventHubs namespace. Typically, you do this only the first time around because the event hubs itself is reusable between runs.

You can create the required resources by using the Azure portal GUI as described in the section `Configuring EventHubs` below. Or quicker, edit and run the included script `scripts/create-eventhubs-instances.ps1`, which automatically creates a resource group, event hubs namespace, and event hubs using Azure CLI commands. The script performs the following commands:

```PowerShell
# edit these parameters before running the script
$groupName="..."
$nameSpaceName="..."
$location='westus'
$partitionCount=12

Write-Host "Creating Resource Group..."
az group create --name $groupName  --location $location

Write-Host "Creating EventHubs Namespace..."
az eventhubs namespace create --name $nameSpaceName --resource-group $groupName 

Write-Host "Creating EventHubs partitions..."
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name partitions --message-retention 1 --partition-count $partitionCount
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients0 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients1 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients2 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $nameSpaceName --resource-group $groupName --name clients3 --message-retention 1 --partition-count 32
```

After you have created the EventHubs, visit the Azure management portal, and under Shared Access Policy, retrieve the connection string for the Eventhubs namespace. Then assign it to the environment variable, like this

```shell
set EventHubsConnection=Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=...
```

Now run the Hello sample again. It uses EventHubs for connecting the client and the partitions.

## As a Function App in Azure

When running Netherite in the cloud, a *premium plan* with fixed minimum node count is recommended for now, until we implement autoscaling.  

To publish the Hello Sample to an Azure Function App, you have to create various Azure resources. To make things simple for this quick start, we made a PowerShell script that creates and configures a function app, and then publishes the hello function app. These are the relevant steps:

```powershell
# edit these parameters before running the script
$name="unique-alphanumeric-name-no-dashes"
$location="westus"
$storageSku="Standard_LRS"
$planSku="EP1"
$numNodes=2

# by default, use the same name for group, function app, storage account, and plan
$groupName=$name
$functionAppName=$name
$storageName=$name
$planName=$name

Write-Host "Creating Resource Group..."
az group create --name $groupName --location $location

Write-Host "Creating Storage Account"
az storage account create --name  $storageName --location $location --resource-group  $groupName --sku $storageSku

Write-Host "Creating and Configuring Function App"
az functionapp plan create --resource-group  $groupName --name  $name --location $location --sku $planSku
az functionapp create --name  $functionAppName --storage-account  $storageName --plan  $functionAppName --resource-group  $groupName --functions-version 3
az functionapp plan update -g  $groupName -n  $functionAppName --max-burst $numNodes --number-of-workers $numNodes --min-instances $numNodes 
az resource update -n  $functionAppName/config/web  -g  $groupName --set properties.minimumElasticInstanceCount=$numNodes --resource-type Microsoft.Web/sites
az functionapp config appsettings set -n $functionAppName -g  $groupName --settings EventHubsConnection=$Env:EventHubsConnection
az functionapp config set -n $functionAppName -g $groupName --use-32bit-worker-process false

Write-Host "Publishing Code to Function App"
func azure functionapp publish $functionAppName
```

To use the script for deploying the Hello sample,

1. Make sure the environment variable `EventHubsConnection` contains the desired value.
1. Edit the name parameters in the script `deploy-to-new-plan.ps1` in the Hello project.
1. Build the Hello project.
1. Go to the directory with the binaries, e.g. `cd samples\Hello\bin\Debug\netcoreapp3.1\`.
1. Run the script there, e.g. by typing `deploy-to-new-plan.ps1` in a shell.

After the script runs successfully, near the bottom of the output, you should see a list of all published functions:

```text
 Functions in <nameofyourfunctionapp>:
    HelloCities - [httpTrigger]
        Invoke url: https://<nameofyourfunctionapp>.azurewebsites.net/api/hellocities

    HelloSequence - [orchestrationTrigger]

    SayHello - [activityTrigger]
```

You can now test the app by issuing an HTTP request to the URL listed for HelloCities, i.e. 

```shell
curl https://<nameofyourfunctionapp>.azurewebsites.net/api/hellocities
```

## Cleanup

Don't forget to delete EventHubs and Plan resources you no longer need, as they will continue to accrue charges.
