# Running the 'Hello' Sample

Here are simple instructions for running the 'Hello' sample project. We included scripts that make it easy to build, run, and deploy this application. Also, this sample is a great resource for understanding how Durable Functions and Netherite work, and an excellent starting point for creating your own projects.

## Prerequisites

First, make sure you have the following installed on your machine:
1. [.NET Core SDK 3.1](https://dotnet.microsoft.com/download/dotnet-core/3.1) or later. 
You can run `dotnet --list-runtimes` to check what is installed.
2. [Azure Functions Core Tools 3.x](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=windows%2Ccsharp%2Cbash) or later.
You can run `func --version` to check what is installed.
3. [The Azure CLI >= 2.18.0](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) or later.
You can run `az --version` to check what is installed.
4. [PowerShell 7.1](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell?view=powershell-7.1) or later.
You can check what is installed with `pwsh --version`. PowerShell is easy to install or update via `dotnet tool install --global PowerShell`.

Also, you need an Azure subscription, to allocate and deploy the required resources.

## Get it and build it

The sample is in the directory [/samples/hello](https://github.com/microsoft/durabletask-netherite/tree/main/samples/Hello). You can download just this folder if you wish, or clone the entire repository. Note that powershell sometimes complains about executing downloaded files; so using git clone may work better.

The project contains a "minimal" .NET Azure Durable Functions project (much like the [DF Quick Start](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-create-first-csharp?pivots=code-editor-visualstudio)):
- a single file of code, `HelloCities.cs`, which defines three functions: a trigger, an orchestration, and an activity.
- a configuration file `host.json` which contains settings.
- a configuration file `local.settings.json` which contains settings for local testing.

Additionally, the folder contains six scripts (files ending in *.ps1) that make it easy to perform typical operations (init, run, deploy, clear, delete) using automated CLI commands. These scripts are purely optional; one can perform all the steps in these scripts manually, using the Azure CLI or the Azure Portal.

To build, open a command shell in that directory and type
```shell
dotnet build
```

You can do this also from within VS code, or Visual Studio, of course.

## Create Azure Resources

First, edit the following line of `settings.ps1`
```PowerShell
$name="globally-unique-lowercase-alphanumeric-name-with-no-dashes"
```
and replace the string with a globally unique string (e.g. `sbnethtest391`) for naming the Azure resources.

**CAUTION:** As this string is used for naming the storage account, it must be between 3 and 24 characters in length and may contain numbers and lowercase letters only.

You may inspect and change any other settings in `settings.ps1` also, if desired.

Next, connect to your Azure subscription:
```shell
az login
```

Then, run the `init.ps1` script. This can take some time:

|Windows|Other|
|-------|-----|
|`pwsh ./init.ps1`|`./init.ps1`|


As it runs, this script creates the following resources:
1. A **storage account** for storing the function application, and the state of orchestrations and entities.
2. An **eventhubs namespace** for providing the persistent queues used by Netherite.
3. A **resource group** that contains the other two resources.

You can inspect these resources in the [Azure Portal](https://portal.azure.com), and change their configuration parameters.

**CAUTION:** The EventHubs namespace incurs continuous charges even if not used. Delete them by running `delete.ps1` once you are done with this sample.

Both the storage account and the EventHubs namespace have a *connection string* that is needed for the application to use them.
Our powershell scripts automatically look them up using the CLI. Alternatively, you can manually set the environment variables `AzureWebJobsStorage` and `EventHubsConnection` to contain these connection strings.

## Run it locally

To start the application on your local machine for debugging, simply run the `run.ps1` script. 

Alternatively, if you prefer to see everything that is happening in detail, you can also do the steps manually:

1. set the environment variables `AzureWebJobsStorage` and `EventHubsConnection` to the respective connection strings
2. enter the directory `bin\Debug\netcoreapp3.1`
2. start the functions runtime with `func start`

Or, if you are in Visual Studio, you can start the local functions debugger as usual, if you set `AzureWebJobsStorage` and `EventHubsConnection` to the respective connection strings before starting Visual Studio.

After executing this script successfully, near the bottom of the output, you should see a list of all three published functions:

```text
Http Functions:

        HelloCities: [GET,POST] http://localhost:7071/api/hellocities
[```

You can now *test the running app* by issuing an HTTP request in a separate terminal:

```shell
curl http://localhost:7071/api/hellocities
```

Which produces the following output, as expected:
```
["Tokyo","Seattle","London"]
```

You may experience some delay initially, as the taskhub needs to be created the first time it is used.

To shut down the functions app down, you can hit Control^C in the terminal which runs it. 

Note that if you leave the app running on your local machine during the next step, it will execute alongside 
with the cluster of machines you run in the cloud! That is, the Netherite load balancer will distribute 
the partitions over all machines in the cloud, and your local machine. While this "super-cluster" would 
function correctly, it is probably not what you want.

## Deploy it to Azure

When running Netherite in the cloud, we use a *premium functions plan* with a fixed node count for now, until we implement autoscaling.  

First, build the release binaries:
```shell
dotnet build -c Release
```

Then, (optionally) review the following lines in `deploy-to-premium.ps1`:

```powershell
# edit these parameters before running the script
$numNodes=2
$planSku="EP1"
```

For demonstration purposes we chose 2 nodes, though 1 node would of course suffice.
We chose the smallest node size (EP1); for heavier loads EP2 or EP3 may be more appropriate.

Finally, run the `deploy-to-premium.ps1` script. This can take some time. As it executes, the script

1. Creates a premium plan with the specified SKU
2. Create a function app
2. Configures the app and plan to use the specified number of nodes
3. Deploys the code to the app

At the end, you should see 

After executing this script successfully, near the bottom of the output, you should see a list of all three published functions:

```text
    HelloCities - [httpTrigger]
        Invoke url: https://<nameofyourfunctionapp>.azurewebsites.net/api/hellocities

    HelloSequence - [orchestrationTrigger]

    SayHello - [activityTrigger]
```

You can now *test the app that is running the cloud* by issuing an HTTP request:

```shell
curl https://<nameofyourfunctionapp>.azurewebsites.net/api/hellocities
```

Which produces the following output, as expected:
```
["Tokyo","Seattle","London"]
```

If you need to update the application, you can rebuild it and redeploy it the same way. You can also change the number of nodes. This allows you to manually scale.

## Cleanup

Don't forget to delete the resources when you're done, as they will continue to accrue charges.

To delete all the Azure Resources we created earlier, just execute the script `delete.ps1` and make sure to confirm your intention by hitting `y`.

**Taskhub only** If you just want to clear the taskhub, but not all the resources, you can run the script `clear.ps`. It deletes the containers in the Azure Blob Storage. You should probably stop the function app before doing so. Also, note that after you do this, you cannot restart the app for some time, because Azure Storage blob containers are unavailable after deletion for some time, maybe 30-60 seconds.
