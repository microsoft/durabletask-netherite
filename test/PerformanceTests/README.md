# Netherite Performance Tests

This project is used to test the performance of various benchmarks, on the Netherite backend and the Azure Storage backend. It also serves the purpose of documenting typical application patterns. 

For simplicity of deployment and scripting, all benchmarks are part of a single function app. For most tests, however, we run only one type of benchmark at a time. Most benchmarks are invoked over REST endpoints of the function app. It is possible to do this interactively, or as part of a batch script that runs for a long time and collects results.

## Directory Organization

The `tests/PerformanceTests` directory is organized as follows:

|Relative Path|Content|
|-|-|
|Benchmarks| contains source code for benchmarks, organized into subfolders. See section *Benchmarks* below. |
|Common| contains source code that is shared by multiple benchmarks. |
|historic| contains `.csv` files with performance results collected over the years. |
|`README.md`| contains general information; this is what you are reading right now. |
|scripts| contains utility scripts for local testing and deploying to the cloud.|
|series| contains scripts to run series of benchmarks in the cloud and collect results in `results.csv`|
|`settings.ps1`| specifies configuration settings for cloud deployments, such as the app name and the region. Is is used by all the scripts.|
|`results.csv`| the file to which the scripts write the measured results. It is automatically created. |

## Standard Procedure for Running a Series

To simplify the collection of data, we have some automatic scripts that create all cloud resources, deploy code, run benchmarks, and delete cloud resources at the end.

NOTE: Running the `wordcount` and `filehash` benchmarks requires permissions for the `gutenbergcorpus` storage account, otherwise these benchmarks fail and their results are blank. 


### Collecting data

Theoretically, running a series of tests is as simple as:

1. Create a branch named `perf/test-X.X.X`
2. Edit `settings.ps1` to specify an app name and a region.
3. Run the desired series, e.g. `pwsh series\across.ps1`
4. Several hours later, inspect the `results.csv`

Unfortunately, in reality it is often not as simple as just going through the above steps. Often, cloud deployments have issues that cause benchmark runs to fail or create bad results. It can help to pick a not-busy datacenter based on the time of day. I usually keep an eye on the results appearing in `results.csv` to see whether they are in the expected range, and abort the run if there are problems. If the results are only partially useful, it makes sense to run or rerun only some of the tests, which is easy to do by temporarily editing the script file, and manually editing the contents of `results.csv`. However, note that when comparing performance (e.g. Netherite vs Azure Storage) we must never mix results from different runs or deployments.

### Analyzing the results

I currently just inspect the `results.csv` file to see whether the number are more or less in line with historic results. 
Perhaps we can add some Jupyter notebooks in the future to make this simpler and more reliable.

### Archiving the results

Once happy with the collected results, 

1. Commit the current branch (which we named `perf/test-X.X.X` earlier) as-is, to serve for future reference
2. Copy the final `results.csv` (or whatever amalgamated file we ended up with) to `historic\results-X.X.X.csv`
3. Switch to the main branch and commit the file `historic\results-X.X.X.csv`

