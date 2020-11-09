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

az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name Partitions --message-retention 1 --partition-count $NumberPartitions
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients0 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients1 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients2 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients3 --message-retention 1 --partition-count 32

echo "Done."