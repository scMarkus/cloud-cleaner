package types

import "sync"

type PartitionProvider interface {
	Init(map[string]interface{}, chan<- error, *sync.WaitGroup)
	MakeRuntimResource(map[string]interface{}) (RuntimeResource, error)
	CheckAccess(chan<- error, *sync.WaitGroup)

	ResourceInputChan() chan<- RuntimeResource
	CollectPartitions(chan<- error, *sync.WaitGroup)
	GetRelatedResources() []RuntimeResource
	GetResourceConcurrency() int
	GetProviderName() string
	GetProviderType() ProviderType
}

type ReplicateProvider interface {
	CopyPartition(partitions PartitionList, source RuntimeResource, target RuntimeResource) (PreparedActions, error)
}

type RemoveProvider interface {
	RemovePartition(partitions PartitionList, source RuntimeResource) (PreparedActions, error)
}
