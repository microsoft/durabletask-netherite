# Instance Placement

Orchestration instances and entity instances are each placed on some particular partition. By design, this placement distributed instance more or less evenly across all partitions, which allows the system to scale out, up to the total number of partitions.

## Hash-based Placement

By default, each instance is placed by *hashing the instance ID*. This instance ID uniquely identifies the orchestration or entity instance:

* For orchestrations, the instance ID is either explicitly specified by the client, or a randomly assigned GUID.
* For entities, the instance ID is of the form `@name@key`, where `name` is the entity name in all-lowercase, and `key` is the entity key.

Using a hash function means that each partition receives approximately the same number of instances, regardless of how instance IDs are chosen by the application. 
However, it also means that applications cannot easily control which partition an instance is placed on.

## Explicit Placement

For some applications it is desirable to exert more control over the placement. For example,

* it may be desirable to make sure each partition receives *exactly* the same number of instances, or
* it may be desirable to place an instance on a particular partition, or
* it may be desirable to place groups of instances on the same partition to optimize the performance of mutual communication.

To support these scenarios, Netherite allows applications to place instances explicitly by using instance IDs that follow a certain syntactic pattern.

### Placement Rules

* An instance ID ending in a suffix of the form `!n` for some nonnegative integer *n* is placed on partition *n*.
* If *n* is larger than the total number of partitions *p*, the instance is placed on partition *n* mod *p*.

### Examples

* An orchestration with instance ID `54f5430d-7b0a-4145-a33e-2949d44b0eea!0` is placed on partition 0.
* If there are at least 8 partitions, an orchestration with instance ID `customer-order-9027403995!7` is placed on partition 7.
* If there are 4 partitions, an orchestration with instance ID `customer-order-9027403995!7` is placed on partition 3.
* If there are at least 8 partitions, an entity with key `mykey!7` or `!7` or `!07` is placed on partition 7.
* All entities with keys ending in `!222` and all orchestrations with instance IDs ending in `!222` are always placed on the same partition, regardless of the total number of partitions.

!> Explicit placement can lead to load imbalance and prevent scale-out if improperly used. For example, placing all instances on the same partition means that only a single node can process orchestration and entity work items.
