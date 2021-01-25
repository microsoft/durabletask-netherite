# EventHubs Configuration

## Partitions

To run a Durable Functions application on Netherite, you must configure an EventHubs namespace that connects clients and partitions using persistent queues. This namespace must contain the following EventHubs:

* An event hub called `partitions` with 1-32 partitions. We recommend 12 as a default.
* Four event hubs called `clients0`, `clients1`, `clients2` and `clients3` with 32 partitions each.

You can create these manually in the Azure portal, or via the Azure CLI:

```shell
az eventhubs eventhub create --namespace-name <namepace> --resource-group <group> --name partitions --message-retention 1 --partition-count 12
az eventhubs eventhub create --namespace-name <namepace> --resource-group <group> --name clients0 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name <namepace> --resource-group <group> --name clients1 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name <namepace> --resource-group <group> --name clients2 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name <namepace> --resource-group <group> --name clients3 --message-retention 1 --partition-count 32
```

You can also edit and run the script `create-eventhubs-instances.ps1` to this end.  


## Partition Count Considerations

In the current implementation, the partition count for an existing TaskHub cannot be changed. Thus, some consideration for choosing this value intially is required.

Setting the partition count to 12 means the system will operate smoothly both for small loads and moderately heavy loads with a scale-out of up to 12 nodes. Setting it to 32 nodes achieves maximum throughput under heavy load, with a scale-out of up to 32 nodes, but can be a little bit less efficient under very small loads (for example, when scaling to or from zero). Unless there is certainty that the system does not need to scale out, we do not recommend setting it to less than 12.

Note that compared to the existing Durable Functions implementation, the partition count is similar to the control queue count; however, unlike the latter, it also affects the maximum scale out for activities (not just orchestrations). For applications that require massive scale for activities, we recommend using HttpTriggers in place of activities.


## Using Multiple TaskHubs

If you use multiple task hubs over time, but not at the same time, you can reuse the same EventHubs namespace. It is not necessary, nor desirable, to clear the contents of the EventHubs streams in between runs: any time a new TaskHub is created, it starts consuming and producing messages relative to the current stream positions. These initial positions are also recorded in the `taskhub.json` file.

Running concurrent TaskHubs on the same EventHubs namespace is not recommmended. In that case, each TaskHub should have its own EventHubs namespace.

## Emulation Mode

For testing or debugging scenarios, we support an emulation mode that eliminates the need for using an actual EventHubs. Since communication is "simulated in memory", this makes sense only for a single node and offers no durability. The emulation mode can be selected by using a pseudo connection string of the form `Memory:n` or `MemoryF:n`, where n is a number in the range 1-32 indicating the number of partitions to use. The difference between the two is that the former emulates the storage also, while the latter exercises the Faster storage component normally.

