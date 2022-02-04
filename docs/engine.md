# Engine Considerations

The Durable Functions and Durable Task frameworks already support a multitude of backends, or execution engines. Choosing the right one depends on many factors that are specific to the intended scenario. Here are some considerations.

**Stronger guarantees**.
Like the other back-ends for DF and DTFx, Netherite provides at-least-once guarantees for executing activities. 
It also provides some additional, stronger guarantees:

- All communication within a TaskHub (i.e. between orchestrations, activities, and entities) is transactionally consistent ("exactly-once"), since the EventHubs API allows us to deliver messages between partitions without any risk of duplication.
- All messages (between clients, orchestrations, activities, and entities) are delivered in order, because this is already guaranteed by EventHubs. For example, when sending a sequence of events or signals to an orchestration or entity, the delivery order is guaranteed to be the same as the sending order. 

**Better performance**.  
Netherite may provide higher throughput and lower latency, compared to the default Azure Storage backend.

- EventHubs can scale up to 32 partitions and 20 throughput units, which allows 20 MB/s worth of messages to be processed on 32 nodes. 
- Page Blobs allows more batching than table storage, which improves maximum throughput.
- Netherite can handle streams of entity signals more efficiently, because there is no need for explicit message sorting and deduplication
- EventHubs supports long polling. This improves latency compared to standard polling on message queues, especially in low-scale situations when polling intervals are high.
- Clients can connect to Netherite via a bidirectional EventHubs connection. This means that a client waiting for an orchestration to complete is notified more quickly.
- Netherite caches instance and entity states in memory, which improves latency if they are accessed repeatedly.
- Multiple orchestration steps can be persisted in a single storage write, which reduces latency when issuing a sequence of very short activities. It can improve throughput by up to 10x.

**Operational complexity**.
Storage configuration and operation for Netherite is a bit more complex than with the default Azure Storage backend.

- Netherite requires users to create an EventHubs namespace prior to running the application.
- The state of instances and orchestrations cannot be inspected using Azure Storage Explorer. Instead, such queries must be directed via the client API to the running service. 

