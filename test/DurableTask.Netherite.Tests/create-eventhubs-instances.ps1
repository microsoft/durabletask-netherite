# This needs to be done only once, to create the eventhubs instances
# From then on, these same instances are reused for all the runs.

# Replace these to match your project
$ResourceGroup = 'MyResourceGroup'
$NameSpace = 'MyEventHubsNameSpace'

# Number of partitions. This can NOT be changed after the taskhub is created,
# so it limits the number of nodes to which Netherite scales out.
$NumberPartitions = 12

# This creates the necessary instances used by this architecture

echo "Creating fresh EventHubs instances..."

az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name partitions --message-retention 1 --partition-count $NumberPartitions
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name clients0 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name clients1 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name clients2 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name clients3 --message-retention 1 --partition-count 32

echo "Done."