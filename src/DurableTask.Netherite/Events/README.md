# Netherite Events

This directory contains the event definitions for the Netherite engine. All components in Netherite communicate via events over an event bus (i.e. Azure Event Hubs). The events are defined as .NET classes that inherit from the `DurableTask.Netherite.Event` abstract base class.

Event classes are organized on the file system based on where they execute. For example `ClientEvents` are events that execute on the client and `LoadMonitor` events are executed on the global load monitor component.

## Event types

The following are the different event types supported by Netherite.

### [Client events](./ClientEvents/)

Client events are events that are sent from the partition and *executed on the client*. In most cases, they are responses to client-initiated requests. For example, a client sends a [StateRequestReceived](./PartitionEvents/External/FromClients/StateRequestReceived.cs) event to a partition to request the state of an orchestration, and the partition responds back to the client with a [StateResponseReceived](./ClientEvents/StateResponseReceived.cs) event, containing that result.

### [Partition events](./PartitionEvents/)

*Partition events* are executed on the partition (i.e. on the worker that owns the partition). They come from clients, the load monitor, or other partitions. Partitions also send events to themselves (internal events). The partition event class definitions are organized on the file system based on where they come from.

There are three types of partition events, which are used to interact with partition state:

1. **Update events**: Atomically read/update one or more tracked objects. These are deterministically replay-able - i.e. it must always have the same effect.
2. **Read events**: Reads a single object.
3. **Query events**: Scans all `InstanceState` objects. These implement the query functionality.

### [Load monitor events](./LoadMonitorEvents/)

Load monitor events are sent by the partition and executed on the global load monitor component.

### [Event fragments](./Fragments/)

(Appears to be a special type of event that allows taking large events and breaking them up into smaller events - more research is needed to confirm)

## Event processing

Netherite operations that modify state are often composed of multiple events in a sequence. This is typically done because partition state needs to be loaded into the cache before it can be modified. For example, the process of creating a new orchestration or entity (instance) involves three steps:

1. **CreateRequestReceived** (phase=`Read`): This *update* event adds itself to the partition's [`PrefetchState`](../PartitionState/PrefetchState.cs) object (which is always in memory) in the target partition. This update ensures that the create request is not lost if the partition goes down.
1. **InstancePrefetch**: This asynchronously fetches the state into the cache and then submits an *update* event to do the actual state update.
1. **StateRequestReceived** (phase=`ConfirmAndProcess`): This *update* event creates the new instance state object in the partition state, sets its status to `Pending`, removes the prior event from the `PrefetchState`, and then enqueues an `ExecutionStartedEvent` which is used by DTFx to actually start the orchestration or entity.

Each of the above steps are atomic, allowing us to reliably recover intermediate state after a crash.

The reason for this design is that the partition state is not always in memory. It is stored in **FasterKV** and is only loaded into memory when needed. If an attempt is made to update some state when it is not in memory, the update will be queued until the state is loaded into memory, stalling the pipeline of (ordered) events the follows the update.

### Partition event pipeline

Events in a Netherite partition are processed in a pipeline that's composed of various "workers" once they are submitted to `IPartitionState.Submit(...)`.

* `IntakeWorker`: Assigns commit log position to each event and decides whether to send the event to a `LogWorker` or a `StoreWorker`.
* `LogWorker`: Continuously persists update events to the commit log using **FasterLog**.
* `StoreWorker`: Handles read/update/query events on the partition state, and periodically/asynchronously checkpoints to **FasterKV**.

This particular design is interesting because it allows events to be processed and persisted in parallel, allowing the system to go faster.
