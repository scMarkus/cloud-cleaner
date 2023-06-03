package resources

import "smartclip.de/cloud-cleaner/types"

type BaseResource struct {
	Name          string
	Provider      types.PartitionProvider
	PartitionSpec []types.PartitionSpec
	Partitions    map[string]types.Partition
}

func (resource *BaseResource) SetProvider(provider types.PartitionProvider) {
	resource.Provider = provider
}

func (resource *BaseResource) GetResourceName() string {
	return resource.Name
}

func (resource *BaseResource) GetProvider() types.PartitionProvider {
	return resource.Provider
}

func (resource *BaseResource) GetPartitionSpec() []types.PartitionSpec {
	return resource.PartitionSpec
}

func (resource *BaseResource) GetPartitions() map[string]types.Partition {
	return resource.Partitions
}

func (resource *BaseResource) IncorporatePartition(newPartition types.Partition) {
	id := newPartition.GetParsedValues().ToString()

	if currentPartition, ok := resource.Partitions[id]; ok {
		currentPartition.UpdatePartition(newPartition)
	} else {
		resource.Partitions[id] = newPartition
	}
}
