# Overview

The results in our submission (523) to the Proceedings of the VLDB Volume 15 (for VLDB 2022) were gathered using two different experimental setups.
For now, we just give an overview of the procedure and artefacts involved. Should the paper be accepted to the next stage, we we will provide more detailed reproduction steps.

## Throughput Experiments

To collect our throughput results, we

1. Deployed the `PerformanceTest` Azure Function App, which contains all the benchmarks, to Azure, within one of the premium plans (EP1, EP2, EP3), and alongside an EventHubs namespace, and Azure Storage.
2. Ran Powershell Scripts on our local machine to repeatedly invoke individual benchmarks in the deployed app. Each of the benchmarks is invoked with a POST command, and returns a JSON object containing the measured time taken and problem size. These results are appended to a local .csv file.
3. After running all the benchmarks, we copied the .csv files to the /results/throughput folder. In fact, this branch contains those files already, in that location.
4. We used jupyter-lab to execute the interactive notebook in that folder (Notebook.ipynb). This generates plots, and exports all the figures used in the paper as pdf files.

Steps 1. and 2. are largely automated: the scripts in /test/series can create all the Azure resources, deploy the function app, run the tests, and generate the .csv files.
This can take several hours for each benchmark. However, things don't always go smooth. Sometimes, deployment steps fail for unknown reasons. Also, datacenter performance can vary a lot at different times of day, and for other unknown reasons. Thus, it is often necessary to retry different parts of the experiment. However, we must always run all the closely related numbers (i.e. all the scale configurations for a single benchmark and single plan size) together, so that the relative performance of those is meaningfully interpretable. I found that selecting a not-so busy-datacenter can help. It's always 2AM somewhere!

## Latency Experiments

To collect latency results, the setup is more complicated since we use separate deployments for the function app and the app under test. Also, some of these experiments run code in AWS, which is not yet included in this repository. Roughly, the steps were as follows:

1. Deployed the `PerformanceTest` Azure Function App (same as for Throughput results), but only for single configuration (4 nodes on EP2). 
   Before doing so, modify host.json to choose between three relevant configurations (original AzureStorage implementation, Netherite with pipelining, Netherite without pipelining)
1. Deployed the `LoadGeneratorApp` Azure Function App to a separate  deployment in the same datacenter, using 10 nodes on EP2.
1. Run the series\runseries.ps1 script, adjusting the "Target" parameter to one of "aws", "af", "neth-p", or "neth-np", depending on what was selected in step 1.
   This runs all the benchmarks that are relevant for that target configuration.
   (Note that step 1 can be entirely skipped for "aws", and "af" does not depend on what configuration was chosen in 1.)
   The results for each individual benchmark are stored in a json file blob container in the storage account whose connection string is in the `ResultConnection` environment variable.
1. After running the tests, open a PowerBI doc to import the relevant data directly from the Azure Storage Account. The doc template is /results/latency/ResultsCollector.pbix. 
   Inside this document, access "Transform Data" to adjust parameters like name of the benchmark, time range for data import. Running the query may require entering credentials. 
1. After viewing the CDF and making adjustments (e.g. exclude some results), choose the export data function on the CDF widget, to create a .csv file.
1. Store these .csv files in /results/latency/data. In fact, this branch contains those files already, in that location. 
1. Run `python3 plot_results.py` to generate pdf plots from the .csv files, ready for including in the paper.




