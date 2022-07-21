# Caching

Netherite uses caching: some instance states and histories are retained in memory even if the corresponding orchestration or entity is not processing any work items at the moment.

Caching can improve overall latency and throughput by loading less data from storage. On the other hand, caching means the application uses more memory, which can leave less memory available for work items, and which can increase cost on consumption plans. To balance these concerns, Netherite keeps the size of the cache below a configurable maximum size.

## Instance Cache

The instance cache keeps the most recently used instance states and instance histories in memory, up to a per-worker specified maximum.
Because the cache size limit is *per worker*, it automatically scales out with the number of workers.

The maximum cache size can be configured using the [InstanceCacheSizeMB configuration parameter](settings.md#orchestration-caching). By default, it is 100MB on consumption plans and (200MB * processorCount) otherwise.

The memory size used by the cache is estimated based on the size of the serialized history and instance states, which can be a bit different than the actual amount of memory used.

!> Limiting the cache to very small sizes is not recommended, because the cache needs to hold the data for the work items currently being processed. Otherwise, *thrashing* may slow the speed at which work items are processed to a crawl.

## Orchestration Cursor Cache

Generally, to process an orchestration work item, a worker has to both fetch the history from storage and replay the orchestrator code.
The instance cache can eliminate the first step by caching the history in memory.

Additionally, Netherite can also cache orchestration cursors: if the in-progress orchestration is still in memory, it can be resumed without replay.

Orchestration Cursor Caching is enabled by default, but can be disabled via the [CacheOrchestrationCursors configuration parameter](settings.md#orchestration-caching).

The memory used by orchestration cursors (i.e. the memory allocated by an in-progress orchestration) is not tracked and does not count towards the cache size maximum.
