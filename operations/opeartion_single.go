package operations

import (
	"log"

	"smartclip.de/cloud-cleaner/types"
)

type OperationSingle struct {
	BaseOperation
	source         types.RuntimeResource
	keptPartitions types.PartitionList
}

func (operation OperationSingle) GetOperationSource() types.RuntimeResource {
	return operation.source
}

func (operation *OperationSingle) GetKeptPartitions() (types.PartitionList, error) {
	if operation.keptPartitions == nil {
		if err := operation.PartitionsWithExcludes(); err != nil {
			return nil, err
		}
	}

	return operation.keptPartitions, nil
}

func (operation *OperationSingle) PartitionsWithExcludes() error {
	// get sorted list of partitions
	partitions := operation.GetOperationSource().GetPartitions()
	partitionCount := len(partitions)
	partitionList := make(types.PartitionList, partitionCount)

	partitionCnt := 0
	for _, partition := range partitions {
		partitionList[partitionCnt] = partition
		partitionCnt++
	}

	var err error
	for _, exclude := range operation.Excludes {
		log.Printf("partition count pre exclude for operation %q: %d", operation.GetOperationName(), len(partitionList))
		if partitionList, err = exclude.IgnorePartition(partitionList); err != nil {
			return err
		}
		log.Printf("partition count after exclude for operation %q: %d", operation.GetOperationName(), len(partitionList))
	}

	operation.keptPartitions = partitionList
	return nil
}
