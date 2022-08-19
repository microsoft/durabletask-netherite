# Basic Scenarios

We now look at some throughput numbers for microbenchmarks that run some simple orchestration, activity, and entity functions.
These provide an estimate of the maximum performance that can be achieved when using the Netherite storage provider.

### Maximum Single-Worker Throughput

The table below shows estimates for the maximum throughput on a single worker, for three different types of worker nodes available with the elastic premium plan (EP1, EP2, EP3).

|Scenario|EP1<br/>1 core|EP2<br/>2 cores|EP3<br/>4 cores|Throughput unit|
|---|---|---|---|---|
A. Run a single orchestration<br/>with sequential activities|250|300|350|activities per second|
B. Run a single orchestration<br/>with parallel activities|500|1000|2000|activities per second|
C. Send signals to a single entity|900|1400|1900|signals per second|
D. Run multiple orchestrations<br/>(each a HelloSequence5)|50|150|300|orchestrations per second|

### Maximum vs. Actual Throughput

The basic scenarios listed in the table above do not perform any interesting work, because the point is to understand the overhead of tracking and persisting the workflow state.  
In particular, the activity functions just echo their input, and the entity operations simply increment a counter.
If your application does any of the following, the throughput would be reduced accordingly:

- Significant computations in activities or entities
- I/O in activities or entities
- Using medium or large size inputs or outputs for orchestrations, activities, or entity operations
- Using medium or large size entity states
- Any additional communication (events, calls, or signals) among clients, orchestrations, and entities
- Any other Azure Functions being triggered on the same workers
- Large latency for Azure Storage or Azure Event Hubs accesses (e.g. due to geographical distance)

### Multi-Worker Scale-Out

For scenarios B and D, which are parallelizable, the throughput can be scaled out by adding more workers.
Ideally we hope to see a linear speedup, i.e. a throughput that increases proportionally to the number of workers.
In reality, the achieved speedup is usually less than linear. The following factors contribute to this effect:

- Load imbalance means not all workers are equally productive at all times.
- More work is expended on communication between multiple workers than when running on a single worker.
- At some point we hit throughput limits for Azure Storage and/or Azure Event Hubs.

?> By design, Netherite optimizes the storage traffic in a way that allows better scale-out than the default Azure Storage provider. For a detailed comparative break-down of the
scale-out performance for scenario D, see the section [Multi-Node Throughput](throughput?id=multi-node-throughput) of the HelloCities5 benchmark.