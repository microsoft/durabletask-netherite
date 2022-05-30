# VLDB 2022 Submission Experiments

This branch documents our experimental results and the code we used for our article *Netherite: Efficient Execution of Serverless Workflows* in the Proceedings of the VLDB Volume 15 (for VLDB 2022). All results were collected in October 2021.

Note that since these experiments use PaaS services that are constantly evolving and changing. Results may vary and the experimental setup may break over time.
We are not maintaining this branch (other than one commit in May 2022 to fix an API change incompability in the DF extension).
*To run experiments with the latest supported version of Netherite, you should use the main branch of this repository.*

The results were gathered using two different experimental setups, one for throughput (Fig. 9), and one for latency (Fig.11).

## Throughput experiments

Running the experiments requires an Azure subscription, and the Azure CLI installed. The following instructions should work on either Linux or Windows.

### Running the 'hello' benchmark

1. Enter `az login` to log into your Azure subscription
1. Enter the `test/PerformanceTests/` directory containing the performance tests.
1. Edit the `settings.ps1` file to choose a unique name for the function app ($name on line 5). Also, you can modify which data center to use ($location on line 8). Selecting a not-so busy-datacenter can help. It's always 2AM somewhere!
1. Execute `pwsh series/hello5.ps1` to execute the script that runs all throughput tests for the hello5 benchmark.
   - As each test runs, its result is appended as a new line to the results.csv file
   - It can take over 8 hours to run through all tests. To run only some of them, edit the script.
   - Unfortunately, things don't always go smooth, so it is necessary to inspect the results.
   Sometimes, deployment steps fail for unknown reasons. Also, datacenter performance can vary a lot at different times of day, and for other unknown reasons.
   - It is therefore often necessary to retry different parts of the experiment. You can manually edit the `results.csv` file for this purpose. Always run all the closely related numbers (i.e. all the scale configurations for a single benchmark and single plan size) together, so that the relative performance of those is meaningfully interpretable.
1. Copy the results to the result directory for later processing:

    `cp results.csv ../../results/throughput/hello.csv`

### Running the other benchmarks

Same, except for the commands in step 4 and 5 where we use

`pwsh series/XXX.ps1`

`cp results.csv ../../results/throughput/XXX.csv`

where `XXX` is one of { `bank`, `collision`, `wordcount` }.

### Generating the graphs

The python notebook `results/throughput/Notebook.ipynb` was used to generate Fig. 9 and Fig. 10.

- For Fig. 9, it parses the results from the generated csv files.
- For Fig. 10, we used manually collected telemetry from the Azure Management Portal.

## Latency experiments

To collect latency results, the setup is more complicated since we use separate deployments for the function app and the app under test. Also, some of these experiments run code in AWS, which is not included in this repository. Roughly, the steps were as follows:

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
