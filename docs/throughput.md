# Throughput testing

The following tests demonstrate the performance characteristics of Netherite, and compare them to the legacy Azure Storage backend. 

**Counting events**. To make throughput easier to compare between benchmarks and backends, we report it as *events per second*. Conceptually, orchestration events correspond to the messages going through persistent queues. For example,

- calling an activity function and handling the response is 2 events
- calling a suborchestrator function and handling the response is 2 events
- calling an entity operation and handling the response is 2 events
- signalling an entity operation is 1 event
- handling an external event is 1 event
- handling an expired timer is 1 event

Event counting is useful for quick back-of-the-napkin calculations to get a rough idea of the throughput requirements for an application. Of course, these are approximations; not all events are in fact equal. For example, the size of the message matters, and the backends contain various optimizations that improve the performance of certain events (such as batching of entity operations).  

**How the experiment is run.** Simply put, we start a number of independent orchestrations and wait for them all to complete. The experiment is started by an Http Trigger function. For large scale experiments, launching all the orchestrations from a single function is too slow, i.e. the speed of starting orchestrations becomes a limiting factor on the throughput. Thus, we use a different method:

|#Orchestrations|Launch Method|
|-|-|
|<10000| start all the orchestrations directly  |
|10000| signal 50 entities that each launches 200 orchestrations |

**How throughput is defined and measured.** For these experiments we define the throughput, in events per second, as 

>  throughput = (events per orchestration) * (number of orchestrations) / (test duration)

To determine the test duration, we look at the start time (creation timestamps) and end time (last-updated timestamp) of each orchestration instance, and compute the test duration by subtracting the earliest creation time from the latest completion time. 

This represents the average throughput over the whole time where orchestrations are executing. This average throughput is typically lower  than then peak throughput because towards the end, fewer orchestrations are running than at the beginning.

## HelloCities5 Benchmark

This benchmark performs a large number of independent orchestrations concurrently. Each orchestration calls 5 activities in sequence. The activities are very short - there is no significant CPU work being done. We use this benchmark as an indicator for the "orchestration step" throughput, i.e. how efficiently orchestration progress is persisted to storage.

> Results may vary due to many factors; we do not claim to guarantee specific numbers. We encourage you to run these tests yourself, and tweak them for your purposes.

### Single-Node Throughput

For the single-node tests, we measure how much throughput a single node achieves, for each backend.

* Number of nodes: **1**
* Scenario: **HelloCities5**
* Number of orchestrations: **1000**
* Events per orchestration: **10**
* Functions runtime: **3.0**
* Region: **West US 2**
* Hosting plan: **[Elastic Premium](https://docs.microsoft.com/azure/azure-functions/functions-premium-plan)**
* Scale setting for EventHubs: **1 TU** (throughput unit)
* Number partitions: **12** for Netherite, **4** for Azure Storage
* Operating system: **Windows**
* Application Insights: **Enabled, warnings only**
* Host configuration: **See the [host.json](https://github.com/microsoft/durabletask-netherite/blob/main/test/PerformanceTests/host.json) file**
* Version: **v0.2.0-alpha**
* Date: **02/08/2021**

<img src="images/hellocities5-singlenode.png" width="600px">

| Compute | Backend | Total time (sec) | Events/sec |
|-|-|-|-|
| EP1 (1-core) | Netherite (1TU) | 17.7 | 565 |
| EP1 (1-core) | Azure Storage   | 31.7 | 315 |
| EP2 (2-core) | Netherite (1TU) | 5.9| 1708 |
| EP2 (2-core) | Azure Storage   | 22.5 | 445 |
| EP3 (4-core) | Netherite (1TU) | 3.0 | 3390 |
| EP3 (4-core) | Azure Storage   | 20.7 | 484 |

Some takeaways:

* Netherite is about 2x faster than Azure Storage on EP1.
* Netherite is about 7x faster than Azure Storage on EP3.
* Netherite on EP1 is faster than Azure Storage on EP3.
* Netherite shows hyperlinear benefits from scaling up.
* A single throughput unit (1TU) is sufficient for all the single-node experiments.


### Multi-Node Throughput

For the single-node tests, we measure how much throughput a single node achieves, for each backend.

* Number of nodes: **4, 8, 12**
* Scenario: **HelloCities5**
* Number of orchestrations: **5000, 10000**
* Events per orchestration: **10**
* Functions runtime: **3.0**
* Region: **West US 2**
* Hosting plan: **[Elastic Premium](https://docs.microsoft.com/azure/azure-functions/functions-premium-plan)**
* Scale setting for EventHubs: **1 TU, 2 TU** (throughput unit)
* Number partitions: **12**
* Operating system: **Windows**
* Application Insights: **Enabled, warnings only**
* Host configuration: **See the [host.json](https://github.com/microsoft/durabletask-netherite/blob/main/test/PerformanceTests/host.json) file**
* Version: **v0.2.0-alpha**
* Date: **02/10/2021**

<img src="images/hellocities5-multinode.png" width="700px">

Some takeaways:

* Compared to Netherite, The Azure Storage backend does not scale up or out very well - throughput improves very little.
* A single throughput unit (1TU) is sufficient for Netherite. We observed throttling in 2 experiments, but it effect was minor, and fixed by adding just one more TU and going to 2TU.
* With Netherite, we observed some load imbalance on 8 nodes (since 12 partitions do not evenly distribute over 8 nodes), thus the 8-node throughput is lower than the average of the 4-node and 12-node throughput.
* At high scale, Netherite outperforms Azure Storage by over an order of magnitude

| Node Size | Node Count | Backend | #Orchestrations | Avg. time (sec) | Events/sec|
|-|-|-|-|-|-|
| EP1 (1-core) | x 4   | Azure Storage | 5000 | 49.2 | 1015 |
| EP1 (1-core) | x 8   | Azure Storage | 5000 | 37.7 | 1326 |
| EP1 (1-core) | x 12  | Azure Storage | 5000 | 34.2 | 1462 |
| EP2 (2-core) | x 4   | Azure Storage | 5000 | 33.8 | 1479 |
| EP2 (2-core) | x 8   | Azure Storage | 5000 | 31.7 | 1578 |
| EP2 (2-core) | x 12  | Azure Storage | 5000 | 31.7 | 1576 |
| EP3 (4-core) | x 4   | Azure Storage | 5000 | 31.6 | 1582 |
| EP3 (4-core) | x 8   | Azure Storage | 5000 | 28.4 | 1760 |
| EP3 (4-core) | x 12  | Azure Storage | 5000 | 26.8 | 1868 |
| EP1 (1-core) | x 4   | Netherite (1 TU) | 5000  | 24.7 | 2027 |
| EP1 (1-core) | x 8   | Netherite (1 TU) | 5000  | 18.3 | 2728 |
| EP1 (1-core) | x 12  | Netherite (1 TU) | 5000  | 16.0 | 3117 |
| EP2 (2-core) | x 4   | Netherite (1 TU) | 10000 | 17.9 | 5601 |
| EP2 (2-core) | x 8   | Netherite (1 TU) | 10000 | 11.0 | 9055 |
| EP2 (2-core) | x 12  | Netherite (1 TU) | 10000 | 5.86  | 17057|
| EP2 (2-core) | x 12  | Netherite (2 TU) | 10000 | 5.75  | 17406|
| EP3 (4-core) | x 4   | Netherite (1 TU) | 10000 | 10.7 | 9354 |
| EP3 (4-core) | x 8   | Netherite (1 TU) | 10000 | 7.09  | 14101|
| EP3 (4-core) | x 12  | Netherite (1 TU) | 10000 | 3.73  | 26828|
| EP3 (4-core) | x 12  | Netherite (2 TU) | 10000 | 3.67  | 27285|


