# Running the 'Hello' Samples

In order to speed up your first experience with Netherite we provide some samples that serve this goal. They are also a great resource for understanding how Durable Functions and Netherite work. In addition you can use them as a starting point for creating your own projects.

All samples are in the folder [/samples/](https://github.com/microsoft/durabletask-netherite/tree/main/samples/). We provide samples for the following languages:

- C#/.NET Core 3.1 in the folder [/samples/Hello_Netherite_with_DotNetCore](https://github.com/microsoft/durabletask-netherite/tree/main/samples/Hello_Netherite_with_DotNetCore)
- TypeScript/Node.js in the folder [/samples/Hello_Netherite_with_TypeScript](https://github.com/microsoft/durabletask-netherite/tree/main/samples/Hello_Netherite_with_TypeScript)
- Python in the folder [/samples/Hello_Netherite_with_Python](https://github.com/microsoft/durabletask-netherite/tree/main/samples/Hello_Netherite_with_Python)

In addition we provide several PowerShell scripts that enable you to setup the necessary resources on Azure as well as to build, run, and deploy the sample applications. You find them in the folder [/samples/scripts](https://github.com/microsoft/durabletask-netherite/tree/main/samples/scripts)

## Prerequisites

### General Prerequisites

First, make sure you have the following installed on your machine:

1. [.NET Core SDK 3.1](https://dotnet.microsoft.com/download/dotnet-core/3.1) or later. You can run `dotnet --list-runtimes` to check what is installed.

2. [Azure Functions Core Tools 3.x](https://docs.microsoft.com/azure/azure-functions/functions-run-local?tabs=windows%2Ccsharp%2Cbash) or later. You can run `func --version` to check what is installed.

3. [PowerShell 7.1](https://docs.microsoft.com/powershell/scripting/install/installing-powershell?view=powershell-7.1) or later. You can check what is installed with `pwsh --version`. PowerShell is easy to install or update via `dotnet tool install --global PowerShell` or `dotnet tool install --global PowerShell`, respectively.

4. [Azurite 3.x](https://github.com/Azure/Azurite) as storage emulator for the local execution. You can run `azurite --version` to check what is installed.

5. [The Azure CLI 2.18.0](https://docs.microsoft.com/cli/azure/install-azure-cli) or later. You can run `az --version` to check what is installed.

6. [An Azure subscription](https://azure.microsoft.com/free/search/), to allocate and deploy the required resources.

To execute the REST calls we recommend to use the [RESTClient for VSCode](https://marketplace.visualstudio.com/items?itemName=humao.rest-client).

📝 **Remark:** You can execute Durable Functions with Netherite local-only without an Azure subscription. In that case you are fine with installing the prerequisites 1. - 4.

### Language-Specific Prerequisites

Depending on the language you want to use, you have some more things to install. You find them in the following sections.

#### C#

You are already good to go, nothing more to install.

#### TypeScript

1. [Node.js](https://nodejs.org/en/download/). Install an LTS version. You can run `node --version` to check what is installed.
2. [TypeScript](https://www.typescriptlang.org/download). You can run `tsc --version` to check what is installed.

#### Python

1. [Python](https://www.python.org/downloads/). Check the [documentation](https://docs.microsoft.com/de-de/azure/azure-functions/supported-languages#languages-by-runtime-version) for the currently supported versions. You can run `python --version` to check what is installed.
2. [Python extension for Visual Studio Code](https://marketplace.visualstudio.com/items?itemName=ms-python.python)

If you are working wth Windows, you can install Python directly form the Microsoft Store e. g. [version 3.8](https://www.microsoft.com/de-de/p/python-38/9mssztt1n39l?rtc=1#activetab=pivot:overviewtab).

## Get it and build it

The samples are in the directory [/samples/](https://github.com/microsoft/durabletask-netherite/tree/main/samples). You can download just this folder if you wish, or clone the entire repository. Note that PowerShell sometimes refuses to execute downloaded files so using git clone may work better.

The language-specific folder in the `samples` folder contain "minimal" Azure Durable Functions projects (much like the [DF Quick Start](https://docs.microsoft.com/azure/azure-functions/durable/durable-functions-create-first-csharp?pivots=code-editor-visualstudio)).

⚠ **CAUTION:** Be aware that all projects have no authentication in place. This makes the calling of the Functions easy, but this is not feasible setup for a production environment.

The build process depends on the language you want to use. The following sections guide you through the specific steps.

### C#

The folder [/samples/Hello_Netherite_with_DotNetCore](https://github.com/microsoft/durabletask-netherite/tree/main/samples/Hello_Netherite_with_DotNetCore) contains the files for the Durable Function App in C#.

The main files and folders are:

- the single file of code, `HelloCities.cs`, which defines three functions: a trigger, an orchestration, and an activity.
- the configuration file `host.json` which contains settings.
- the configuration file `local.settings.json` which contains settings for local execution.

Additionally, the folder contains PowerShell scripts (files ending in `*.ps1`) that make it easy to run and deploy the Function using automated CLI commands. These scripts are optional. You can perform all the steps in these scripts manually, using the Azure CLI or the Azure Portal.

To build, open a command shell in that directory and type

```powershell
dotnet build
```

### TypeScript

The folder [/samples/Hello_Netherite_with_TypeScript](https://github.com/microsoft/durabletask-netherite/tree/main/samples/Hello_Netherite_with_TypeScript) contains the files for the Durable Function App in TypeScript.

The main files and folders are:

- the folders containing the code (`*.ts`) and configuration (`function.json`) of the three functions: the trigger (`DurableFunctionsNetheriteStarter`), the orchestrator (`DurableFunctionsNetheriteOrchestrator`), and the activity (`HelloCityNetherite`).
- the configuration file `host.json` which contains settings.
- the configuration file `local.settings.json` which contains settings for local execution.
- the `extensions.csproj` file that defines the needed extension for Durable Functions and Netherite as we cannot rely on the Extension Bundle mechanism.
- the `package.json` file containing the metadata of the project and the dependencies
- the `tsconfig.json` file containing the compiler options for TypeScript

Additionally, the folder contains PowerShell scripts (files ending in `*.ps1`) that make it easy to run and deploy the Function using automated CLI commands. These scripts are optional. You can perform all the steps in these scripts manually, using the Azure CLI or the Azure Portal.

To build, open a command shell in that directory and type

```powershell
npm install

func extensions install

npm run build
```

### Python

The folder [/samples/Hello_Netherite_with_TypeScript](https://github.com/microsoft/durabletask-netherite/tree/main/samples/Hello_Netherite_with_TypeScript) contains the files for the Durable Function App in Python.

The main files and folders are:

- the folders containing the code (`*.py`) and configuration (`function.json`) of the three functions: the trigger (`DurableFunctionsNetheriteStarter`), the orchestrator (`DurableFunctionsNetheriteOrchestrator`), and the activity (`HelloCityNetherite`).
- the configuration file `host.json` which contains settings.
- the configuration file `local.settings.json` which contains settings for local execution.
- the `extensions.csproj` file that defines the needed extension for Durable Functions and Netherite as we cannot rely on the Extension Bundle mechanism.
- the `requirements.txt` file containing the dependencies

Additionally, the folder contains PowerShell scripts (files ending in `*.ps1`) that make it easy to run and deploy the Function using automated CLI commands. These scripts are optional. You can perform all the steps in these scripts manually, using the Azure CLI or the Azure Portal.

To build, open a command shell in that directory and type

```powershell
py -m venv .venv

pip install -r requirements.txt

func extensions install
```

## Run it locally

We can now give it a first try and run the Functions locally without using resources on Azure. To do so start the Azurite storage emulator (e. g. via the [VS Code plugin](https://marketplace.visualstudio.com/items?itemName=Azurite.azurite)).

Each language-specific folder contains a pre-configured `local.settings.json` file that contains the fitting parameters for a local execution namely:

```json
"AzureWebJobsStorage": "UseDevelopmentStorage=true",
"EventHubsConnection": "MemoryF",
```

When the storage emulator is up and running you can start the Functions depending on your language of choice.

### C#

- go to the directory `bin\Debug\netcoreapp3.1` within the folder `Hello_Netherite_with_DotNetCore`.
- start the Functions runtime with `func start`.

### TypeScript

- go to the directory `Hello_Netherite_with_TypeScript`.
- start the functions runtime via `npm run start`.

### Python

- go to the directory `Hello_Netherite_with_Python`.
- start the Functions runtime with `func start`.

### Debug Mode

You can also start the Functions in debugging mode by opening the language-specific directory in VS Code/Visual Studio and pressing `F5`. You can then also set breakpoints.

### Call the Function

As soon as you have started the Function you will see an endpoint in the logs to trigger the Function execution. We have prepared the script `request.http` in [/samples/scripts](https://github.com/microsoft/durabletask-netherite/tree/main/samples/scripts) based on the VS Code REST extension to call the endpoints.

You can also use Postman or CURL to call the invocation endpoints via a *POST* request:

- C#: `http://localhost:7071/api/hellocities`
- TypeScript and Python: `http://localhost:7071/api/orchestrators/DurableFunctionsNetheriteOrchestrator`

This should trigger any breakpoints you have set, and basically produce the following output:

```json
["Tokyo","Seattle","London"]
```

Be aware that you must query the `statusUri` for non.NET languages to get the result including additional status information.

### Cleanup

To shut down the functions app, hit `Control^C` in the terminal which runs it. You can also stop Azurite.

🚀 **Great** - you have successfully executed Durable Functions with Netherite locally. Let us move on and combine this with the necessary resources on Azure.

## Create Azure Resources

First, edit the following line of `settings.ps1` in the folder [/samples/scripts](https://github.com/microsoft/durabletask-netherite/tree/main/samples/scripts)

```powershell
$name="globally-unique-lowercase-alphanumeric-name-with-no-dashes"
```

and replace the string with a globally unique string (e. g. `sbnethtest391`) for naming the Azure resources.

⚠ **CAUTION:** As this string is used for naming the storage account, it must be between 3 and 24 characters in length and may contain numbers and lowercase letters only.

You may inspect and change any other settings in `settings.ps1` also, if desired.

Next, connect to your Azure subscription:

```powershell
az login
```

Then, run the `init.ps1` script. This can take some time:

|Windows          |Other       |
|-------          |-----       |
|`pwsh ./init.ps1`|`./init.ps1`|

As it runs, this script creates the following resources:

1. A **resource group** for the resources.
2. A **storage account** for storing the Function application, and the state of orchestrations and entities.
3. An **EventHubs namespace** for providing the persistent queues used by Netherite.

You can inspect these resources in the [Azure Portal](https://portal.azure.com), and change their configuration parameters.

⚠ **CAUTION:** The EventHubs namespace incurs continuous charges even if not used. Make sure to clean up resources by running `delete.ps1` once you are done with this sample.

Both the storage account and the EventHubs namespace have a *connection string* that is needed for the application to use them. Our PowerShell scripts automatically look them up using the CLI. Alternatively, you can manually set the environment variables `AzureWebJobsStorage` and `EventHubsConnection` to contain these connection strings or put them into the `local.settings.json` file.

## Run or Debug hybrid

As we have the needed resources in place we can now execute our Functions using the storage account and the EventHub on Azure.

You have two options now:

1. Set the environment variables `AzureWebJobsStorage` and `EventHubsConnection` to contain these connection strings or put them into the `local.settings.json` file and start the Function as for the local execution before.
2. Execute the pre-configured scripts in the language-specific folders:
    - C#: run-dotnet-function-hybrid.ps1
    - TypeScript: run-typescript-function-hybrid.ps1
    - Python: run-python-function-hybrid.ps1

You may experience some delay initially, as the taskhub needs to be created the first time it is used.

Trigger the Function via the HTTP request as in the local execution scenario. 

### Cleanup

To shut down the functions app down, you can hit `Control^C` in the terminal which runs it.

⚠ **CAUTION:** If you leave the app running on your local machine during the next step, it will execute alongside with the cluster of machines you run in the cloud! That is, the Netherite load balancer will distribute the partitions over all machines in the cloud, and your local machine. While this "super-cluster" would function correctly, it is probably not what you want.

## Deploy it to Azure

Now it is time to bring the Function App to Azure and run it in the cloud. The deployment to Azure comprises two steps:

- Creation of the Function App on Azure
- Deployment of Azure Function to the Function App on Azure.

We will walk through these steps in the next two sections.

📝 **Remark:** We use a *premium functions plan* for now, until we implement support for the consumption plan.

### Setup Azure Functions App on Azure

The setup of the Azure Functions app is language-specific. We provide a PowerShell script for the samples in the folder [/samples/scripts](https://github.com/microsoft/durabletask-netherite/tree/main/samples/scripts) namely:

- C#: `create-function-app-dotnet.ps1`
- TypeScript: `create-function-app-node.ps1`
- Python: `create-function-app-python.ps1`

These scripts will:

1. create a premium plan with the specified SKU
2. create a Function App
3. configure the Function App for Netherite

Before you execute the scripts, review and if applicable change the following parameters in the script (as default we chose the smallest node size _EP1_. For heavier loads EP2 or EP3 may be more appropriate.):

```powershell
param (
	$Plan = "EP1", 
	$MinNodes = "1", 
	$MaxNodes = "20", 
    ...
)
```

After you have executed the script you can deploy the Function.

### Deploy Azure Function to Azure

You can trigger the deployment of the Function via the corresponding PowerShell script in the language-specific sample folder:

- C#: `deploy-dotnet-function.ps1`
- TypeScript: `deploy-typescript-function.ps1`
- Python: `deploy-python-function.ps1`

These scripts build and deploy you Function to the Function App created in the previous section.

After the script has finished successfully, you should see a list of all three published functions near the bottom of the output including the `Invoke url` for the HTTP trigger.

Here is an example output for .NET:

```powershell
    HelloCities - [httpTrigger]
        Invoke url: https://<nameofyourfunctionapp>.azurewebsites.net/api/hellocities

    HelloSequence - [orchestrationTrigger]

    SayHello - [activityTrigger]
```

📝 **Remark** You find the URL also in the Azure portal. For the Python-based deployment this is the preferred way, as the output will not show up.  

You can now *test the app that is running the cloud* by issuing an HTTP request to the corresponding invocation endpoint. This should produce the same output as in the case of the local or hybrid scenarios executed before.

If you need to update the application, you can rebuild it and redeploy it the same way. You can also change MinNodes or MaxNodes. This allows you to manually scale to a desired number.

## Cleanup Azure Resources

Don't forget to delete the resources when you're done, as they will continue to accrue charges.

To delete all the Azure resources, execute the script `delete.ps1` and make sure to confirm your intention by hitting `y`.

### Taskhub only

If you just want to clear the taskhub, but not delete the resources, you can run the script `clear.ps`. It deletes the containers in the Azure Blob Storage. You should stop the function app before doing so.

Note that after you do this, you cannot restart the app for some time, because Azure Storage blob containers are unavailable after deletion for some time (~ 30-60 seconds).

## Walk Through on YouTube 🎥

If you want to follow along migrating a Durable Function from Azure Storage to Netherite, we recommend the YouTube video [Migrate your Durable Function to Netherite](https://youtu.be/GRcHeZkmVcM). Using *TypeScript* for the demo the video covers the following steps:

| Step | Content                                                                                                 | Link to chapter of video
|---   |---                                                                                                      | ---
| 1    | Introduction and local setup of Durable Function                                                        | [Link](https://www.youtube.com/watch?v=GRcHeZkmVcM&t=0s)
| 2    | Migration of Durable Function configuration to Netherite and local validation of the new configuration  | [Link](https://www.youtube.com/watch?v=GRcHeZkmVcM&t=299s)
| 3    | Setup of Azure resources (Event Hub and Azure Storage) and hybrid validation of the setup               | [Link](https://www.youtube.com/watch?v=GRcHeZkmVcM&t=1019s)
| 4    | Deployment of Azure Durable Function and execution on Azure                                             | [Link](https://www.youtube.com/watch?v=GRcHeZkmVcM&t=1596s)
| 5    | Cleanup of Azure resources                                                                              | [Link](https://www.youtube.com/watch?v=GRcHeZkmVcM&t=2266s)

>> 🔎  The video uses the Netherite extension in version 0.5.0-alpha. By the time you will watch this video, there might be a newer version available. The migration procedure *per se* should remain the same.
