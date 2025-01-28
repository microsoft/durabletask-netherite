# Partition state

A partition is a collection of `TrackedObject` objects whose state is tracked by event sourcing. There are two types of objects stored in a partition:

1. **Singleton state objects**: These are large objects that internally contain collections. For example, `TimerState` contains a collection of all timers in the partition.
2. **Per-instance state objects**: These are defined per instance ID and contain the instance state and the history state.

## Partition state objects

The following are the partition state objects managed by Netherite:

| Object            | Type      | Description                                                      |
|-------------------|-----------|------------------------------------------------------------------|
| `InstanceState`   | Instance  | Contains the state of an orchestration or entity instance.       |
| `HistoryState`    | Instance  | Stores the history of one orchestration or entity instance.      |
| `ActivitiesState` | Singleton | Contains the state of all activities in the partition (in-progress or queued). |
| `DedupState`      | Singleton | Stores deduplication vector for messages from other partitions.  |
| `OutboxState`     | Singleton | Buffers all outgoing messages and responses.                     |
| `PrefetchState`   | Singleton | Buffers client requests until state is in memory.                |
| `QueriesState`    | Singleton | Buffers client query requests.                                   |
| `ReassemblyState` | Singleton | Buffers received fragments until reassembled.                    |
| `SessionState`    | Singleton | Stores all orchestration and entity instance messages (in-progress or queued). |
| `StatsState`      | Singleton | Stores the list and counts of all instances in the partition.    |
| `TimersState`     | Singleton | Buffers instance messages scheduled for future delivery.         |

Each partition has its own isolated collection of these state objects.

## Updating partition state

Partition state is updated by submitting events to the partition state object. The partition state object is responsible for applying the event to the state and updating the state in memory. The in-memory state is then periodically persisted to FasterKV.

See [../Events/README.md](../Events/README.md) for more information on the different types of events that can be submitted to the partition state.
