# EventHubs Configuration

## Creating the Event Hubs

To run a Durable Functions application on Netherite, you must first (a) create an **EventHubs namespace**, and (b) create certain **EvenHubs** in that namespace. Specfically, the namespace is expected to contain the following event hubs:

* **An event hub called `partitions`** with 1-32 partitions.

  | value | indication |
  |-------|------------|
  | 12 | Performs well across a range of 1-12 nodes. *This is the recommended default*. |
  | 32 | Permits maximal scaleout of up to 32 nodes, but is not recommended for running on a single node with less than 8 cores. |
  | 1  | Achieves optimal performance on a single node. Useful only if there is no intention to *ever* scale out. |

* **Four event hubs called `clients0`, `clients1`, `clients2` and `clients3`** with 32 partitions each.

  These are used for sending responses back to the clients. By design, we use vastly more partitions (128) than the expected number of clients.
  The reason is that clients hash to these partitions randomly, and we want reduce the likelihood of hash conflicts.

  
### Manually, in the portal

You can create the necessary event hubs manually in the portal, by following these steps:

1. Go to the [online Azure Portal](https://ms.portal.azure.com)
2. Choose "Create a resource"
3. Type "Event Hubs" into the search box, then click it in the suggested search completions
4. On the "Event Hubs" page, click the "Create" button
5. Choose any namespace name, and tweak other options if desired (e.g. select an existing or new resource group)
6. Click "Review + create"
7. When complete, click "Go to resource"
8. Click on "+ Event Hub"
9. Enter "partitions" for the Name, and set the partition count to 12
9. Hit "Create"
8. Click on "+ Event Hub"
9. Enter "clients0" for the Name, and set the partition count to 32
9. Hit "Create"
8. Click on "+ Event Hub"
9. Enter "clients1" for the Name, and set the partition count to 32
9. Hit "Create"
8. Click on "+ Event Hub"
9. Enter "clients2" for the Name, and set the partition count to 32
9. Hit "Create"
8. Click on "+ Event Hub"
9. Enter "clients3" for the Name, and set the partition count to 32
9. Hit "Create"

### Via Azure CLI

If you don't prefer using a graphical interface, or if you want to automate this process, we
recommend using the Azure CLI commands to perform the configuration.

If you don't already have a resource group you want to use, create it first:

```shell
az group create --resource-group <group> --location <location>
```

Then, create the eventhubs namespace:
```shell
az eventhubs namespace create --namespace-name <namespace> --resource-group <group> 
```

Finally, create the eventhubs:
```shell
az eventhubs eventhub create --namespace-name <namespace> --resource-group <group> --name partitions --message-retention 1 --partition-count 12
az eventhubs eventhub create --namespace-name <namespace> --resource-group <group> --name clients0 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name <namespace> --resource-group <group> --name clients1 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name <namespace> --resource-group <group> --name clients2 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name <namespace> --resource-group <group> --name clients3 --message-retention 1 --partition-count 32
```
You can also edit and run the script `create-eventhubs-instances.ps1` to this end.  

### Cleanup

EventHubs accumulate charges for the reserved capacity (1 throughput unit, or TU, by default) even if not used.
So, don't forget to delete EventHubs and Plan resources you no longer need, as they will continue to accrue charges.

This is easiest if you use a resource group to contain all your resource (which may also contain your function app and Azure Storage account),
and then delete that entire group in the portal, or from the CLI:

```shell
az group delete --resource-group <group>
```

## Partition Count Considerations

In the current implementation, the partition count for an existing TaskHub cannot be changed. Thus, some consideration for choosing this value intially is required.

Setting the partition count to 12 means the system will operate smoothly both for small loads and moderately heavy loads with a scale-out of up to 12 nodes. Setting it to 32 nodes achieves maximum throughput under heavy load, with a scale-out of up to 32 nodes, but can be a little bit less efficient under very small loads (for example, when scaling to or from zero). Unless there is certainty that the system does not need to scale out, we do not recommend setting it to less than 12.

Note that compared to the existing Durable Functions implementation, the partition count is similar to the control queue count; however, unlike the latter, it also affects the maximum scale out for activities (not just orchestrations). For applications that require massive scale for activities, we recommend using HttpTriggers in place of activities.


## Using Multiple TaskHubs

If you use multiple task hubs over time, but not at the same time, you can reuse the same EventHubs namespace. It is not necessary, nor desirable, to clear the contents of the EventHubs streams in between runs: any time a new TaskHub is created, it starts consuming and producing messages relative to the current stream positions. These initial positions are also recorded in the `taskhub.json` file.

Running concurrent TaskHubs on the same EventHubs namespace is not recommmended. In that case, each TaskHub should have its own EventHubs namespace.

## Emulation Mode

For testing or debugging scenarios, we support an emulation mode that eliminates the need for using an actual EventHubs. Since communication is "simulated in memory", this makes sense only for a single node and offers no durability. The emulation mode can be selected by using a pseudo connection string of the form `Memory:n` or `MemoryF:n`, where n is a number in the range 1-32 indicating the number of partitions to use. The difference between the two is that the former emulates the storage also, while the latter exercises the Faster storage component normally.

