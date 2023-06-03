package exclude

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/SiverPineValley/parseduration"
	"smartclip.de/cloud-cleaner/types"
)

type directedPartitionTimeFromTo struct {
	fromGreatest bool
	amount       time.Duration
}

type PartitionTimeExclude struct {
	from directedPartitionTimeFromTo
	to   directedPartitionTimeFromTo
}

func (excludeSpec PartitionTimeExclude) IgnorePartition(partitions types.PartitionList) (types.PartitionList, error) {
	keptPartitions := make(types.PartitionList, len(partitions))
	partitionKeepCnt := 0
	var (
		currentPartitionTs, greatestPartitionTs, smallesPartitiontTs, from, to time.Time
		err                                                                    error
	)

	sort.Sort(partitions)
	smallest := partitions[0]
	greatest := partitions[len(partitions)-1]

	if greatestPartitionTs, err = greatest.GetTimestamp(); err != nil {
		return types.PartitionList{}, err
	}
	if smallesPartitiontTs, err = smallest.GetTimestamp(); err != nil {
		return types.PartitionList{}, err
	}

	if excludeSpec.from.fromGreatest {
		from = greatestPartitionTs.Add(excludeSpec.from.amount)
	} else {
		from = smallesPartitiontTs.Add(excludeSpec.from.amount)
	}
	if excludeSpec.to.fromGreatest {
		to = greatestPartitionTs.Add(excludeSpec.to.amount)
	} else {
		to = smallesPartitiontTs.Add(excludeSpec.to.amount)
	}

	log.Printf("partition timestamp exclude from: %q - to: %q", from.Format(time.RFC3339), to.Format(time.RFC3339))
	for _, partition := range partitions {
		if currentPartitionTs, err = partition.GetTimestamp(); err != nil {
			return types.PartitionList{}, err
		}

		excludePartition := currentPartitionTs.After(from) && currentPartitionTs.Before(to)
		tmp, _ := partition.GetTimestamp()
		log.Printf("partition %q - exclude: %t", tmp.Format(time.UnixDate), excludePartition)
		if !excludePartition {
			keptPartitions[partitionKeepCnt] = partition
			partitionKeepCnt++
		}
	}

	return keptPartitions[:partitionKeepCnt], nil
}

func MakePartitionTimestampExclude(operationName string, conf map[string]interface{}) (types.Exclude, error) {
	var (
		err     error
		exclude PartitionTimeExclude
	)

	if val, ok := conf["from"]; ok {
		tmp, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("\"from\" field in exclude of operation %q is not a string", operationName)
		}
		if strings.HasPrefix(tmp, "-") {
			exclude.from.fromGreatest = true
		}
		if exclude.from.amount, err = parseduration.ParseDuration(tmp); err != nil {
			return nil, err
		}
	}

	if val, ok := conf["to"]; ok {
		tmp, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("\"to\" field of operation %q is not a string", operationName)
		}
		if strings.HasPrefix(tmp, "-") {
			exclude.to.fromGreatest = true
		}

		if exclude.to.amount, err = parseduration.ParseDuration(tmp); err != nil {
			return nil, err
		}
	} else {
		exclude.to.fromGreatest = true
	}

	return exclude, nil
}
