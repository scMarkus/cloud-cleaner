package types

type Exclude interface {
	IgnorePartition(partitions PartitionList) (PartitionList, error)
}
