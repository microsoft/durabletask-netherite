# Instance Placement

Orchestration instances and entity instances are distributed across Netherite partitions, which allows the system to scale out.

## Hash-based Placement

By default, the placement of each instance is determined by a hash of the instance ID string:

* For orchestrations, the instance ID is either explicitly specified by the client, or a randomly assigned GUID.
* For entities, the instance ID is of the form `@name@key`, where `name` is the entity name in all-lowercase, and `key` is the entity key.

Using a hash function means that each partition receives approximately the same number of instances, regardless of how instance IDs are chosen by the application.

## Explicit Placement

For some applications it can be desirable to override the default hash-based placement. For example,

* it may be desirable to make sure each partition receives *exactly* the same number of instances
* it may be desirable to place an instance on a particular partition
* it may be desirable to place groups of instances on the same partition to optimize the performance of mutual communication

To support these scenarios, Netherite allows applications to place instances explicitly, rather than relying on a hash function.

### Placement Suffix for instance IDs

Any instance ID ending in a suffix of the form `!n` for some nonnegative integer *n* is placed on partition *n*.
If *n* is larger than the number of partitions, it wraps around.

For example:

* An orchestration with instance ID `54f5430d-7b0a-4145-a33e-2949d44b0eea!0` is placed on partition 0.
* If there are at least 8 partitions, an orchestration with instance ID `customer-order-9027403995!7` is placed on partition 7.
* If there are 4 partitions, an orchestration with instance ID `customer-order-9027403995!7` is placed on partition 7.
* If there are at least 8 partitions, an entity with key `mykey!7` or `!7` or `!07` is placed on partition 7.
* All entities with keys ending in `!222` and all orchestrations with instance IDs ending in `!222` are placed on the same partition.

!> Explicit placement can lead to load imbalance and prevent scale-out if improperly used. For example, placing all instances on the same partition means that only a single node can process orchestration and entity work items.
