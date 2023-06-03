package partitions

import (
	"log"
	"sync"

	"smartclip.de/cloud-cleaner/types"
)

type BasePartition struct {
	PartitionValues      []string
	TypedPartitionValues types.TypedPartitionValueList
	dependencies         types.PartitionDependencies
	Resource             types.RuntimeResource
	CompletionWg         *sync.WaitGroup
}

func (partition *BasePartition) GetValues() []string {
	return partition.PartitionValues
}

func (partition *BasePartition) GetDependencies() types.PartitionDependencies {
	return partition.dependencies
}

func (partition *BasePartition) GetParsedValues() types.TypedPartitionValueList {
	return partition.TypedPartitionValues
}

func (currentPartition *BasePartition) AddDependencies(wg *sync.WaitGroup) {
	currentPartition.dependencies = append(currentPartition.dependencies, wg)
}

func (partition *BasePartition) RegisterCompletionLock() *sync.WaitGroup {
	return partition.CompletionWg
}

func (partition *BasePartition) WaitForCompletion() {
	partition.CompletionWg.Add(1)
}

func (partition *BasePartition) CloseCompleteChan() {
	if partition.CompletionWg != nil {
		log.Printf("unblocking partition: %s", partition.PartitionValues)
		partition.CompletionWg.Done()
	}
}
