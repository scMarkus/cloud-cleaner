package exclude

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/SiverPineValley/parseduration"
	"smartclip.de/cloud-cleaner/types"
)

type CurrentTimeExclude struct {
	from time.Duration
	to   time.Duration
}

func (excludeSpec CurrentTimeExclude) IgnorePartition(
	partitions types.PartitionList,
) (types.PartitionList, error) {
	keptPartitions := make(types.PartitionList, len(partitions))
	partitionKeepCnt := 0

	currentTs := time.Now()
	from := currentTs.Add(excludeSpec.from)
	to := currentTs.Add(excludeSpec.to)

	log.Printf("current timestamp exclude from: %q - to: %q", from.Format(time.RFC3339), to.Format(time.RFC3339))
	for _, partition := range partitions {
		partitionTs, err := partition.GetTimestamp()
		if err != nil {
			return types.PartitionList{}, err
		}

		excludePartition := partitionTs.After(from) && partitionTs.Before(to)
		if !excludePartition {
			keptPartitions[partitionKeepCnt] = partition
			partitionKeepCnt++
		}
	}

	return keptPartitions[:partitionKeepCnt], nil
}

func MakeCurrentTimestampExclude(operationName string, conf map[string]interface{}) (types.Exclude, error) {
	var (
		err     error
		exclude CurrentTimeExclude
	)

	if val, ok := conf["from"]; ok {
		tmp, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("\"from\" field in exclude of operation %q is not a string", operationName)
		}
		if exclude.from, err = parseduration.ParseDuration(tmp); err != nil {
			return nil, err
		}
	} else {
		exclude.from = time.Duration(math.MinInt64)
	}

	if val, ok := conf["to"]; ok {
		tmp, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("\"to\" field of operation %q is not a string", operationName)
		}
		if exclude.to, err = parseduration.ParseDuration(tmp); err != nil {
			return nil, err
		}
	} else {
		exclude.to = time.Duration(math.MaxInt64)
	}

	return exclude, nil
}
