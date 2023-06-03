package types

// this interface is a dummy to unify various resources which get there concrete type by a provider
type RuntimeResource interface {
	IncorporatePartition(newPartition Partition)
	SetProvider(PartitionProvider)

	GetResourceName() string
	GetProvider() PartitionProvider
	GetPartitionSpec() []PartitionSpec
	GetPartitions() map[string]Partition
}
