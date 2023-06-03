package exclude

import (
	"fmt"
	"log"
	"time"

	"smartclip.de/cloud-cleaner/types"
)

type AbsoluteTimeExclude struct {
	from time.Time
	to   time.Time
}

func (excludeSpec AbsoluteTimeExclude) IgnorePartition(
	partitions types.PartitionList,
) (types.PartitionList, error) {
	keptPartitions := make(types.PartitionList, len(partitions))
	partitionKeepCnt := 0

	log.Printf("current timestamp exclude from: %q - to: %q", excludeSpec.from.Format(time.RFC3339), excludeSpec.to.Format(time.RFC3339))
	for _, partition := range partitions {
		ts, err := partition.GetTimestamp()
		if err != nil {
			return types.PartitionList{}, err
		}

		excludePartition := ts.After(excludeSpec.from) && ts.Before(excludeSpec.to)
		if !excludePartition {
			keptPartitions[partitionKeepCnt] = partition
			partitionKeepCnt++
		}
	}

	return keptPartitions, nil
}

func MakeAbsoluteTimestampExclude(operationName string, conf map[string]interface{}) (types.Exclude, error) {
	var (
		err     error
		exclude AbsoluteTimeExclude
	)

	val, ok := conf["from"]
	if !ok {
		tmp, ok := val.(string)
		if ok {
			return nil, fmt.Errorf("\"from\" field in exclude of operation %q is not a string", operationName)
		}
		if exclude.from, err = time.Parse("2006-01-02T15:04:05.000Z", tmp); err != nil {
			return nil, err
		}
	} else {
		exclude.from = time.Unix(0, 0)
	}

	val, ok = conf["to"]
	if !ok {
		tmp, ok := val.(string)
		if ok {
			return nil, fmt.Errorf("\"to\" field in exclude of operation %q is not a string", operationName)
		}
		if exclude.to, err = time.Parse("2006-01-02T15:04:05.000Z", tmp); err != nil {
			return nil, err
		}
	} else {
		exclude.to = time.Unix(1<<63-1, 999999999)
	}

	return exclude, nil
}
