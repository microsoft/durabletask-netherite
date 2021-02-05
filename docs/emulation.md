# Emulation Mode

For testing or debugging scenarios, we support an emulation mode that eliminates the need for using an actual EventHubs. 

Since communication is "simulated in memory", this makes sense only for a single node. Also, it does not guarantee reliable execution.

The emulation mode can be selected by using a "pseudo-connection-string" instead of a real Event Hubs onnection string:

|String | Interpretation |
|--|--|
|Memory| simulate both the queues and the partition states in memory |
|MemoryF| simulate the queues in memory, but store the partition states in Azure Storage |

