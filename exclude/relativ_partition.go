package exclude

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"smartclip.de/cloud-cleaner/types"
)

type directedRelativPartitionFromTo struct {
	fromGreatest bool
	amount       int
}

type RelativPartitionExclude struct {
	from directedRelativPartitionFromTo
	to   directedRelativPartitionFromTo
}

func (excludeSpec RelativPartitionExclude) IgnorePartition(partitions types.PartitionList) (types.PartitionList, error) {
	var from, to int

	sort.Sort(partitions)

	if excludeSpec.from.fromGreatest {
		from = len(partitions) + excludeSpec.from.amount // amount can be negative
	} else {
		from = excludeSpec.from.amount
	}

	if excludeSpec.to.fromGreatest {
		to = len(partitions) + excludeSpec.to.amount // amount can be negative
	} else {
		to = excludeSpec.to.amount
	}

	if from > len(partitions) {
		from = len(partitions)
	}
	if from < 0 {
		from = 0
	}
	if to > len(partitions) {
		to = len(partitions)
	}
	if to < 0 {
		to = 0
	}

	keptUpper := partitions[to:]
	keptLower := partitions[:from]
	keptPartitions := append(keptLower, keptUpper...)
	if len(keptPartitions) > 0 {
		log.Printf(
			"relative partitions exclude keeps from: %q, to: %q, cnt: %d",
			keptPartitions[0].GetParsedValues().ToString(),
			keptPartitions[len(keptPartitions)-1].GetParsedValues().ToString(),
			len(keptPartitions),
		)
	}

	return keptPartitions, nil
}

func MakeRelativPartitionExclude(operationName string, conf map[string]interface{}) (types.Exclude, error) {
	var (
		err     error
		exclude RelativPartitionExclude
	)

	val, ok := conf["from"]
	if ok {
		tmp, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("\"from\" field in excludes of operation %q is not a string", operationName)
		}
		if strings.HasPrefix(tmp, "-") {
			exclude.from.fromGreatest = true
		}
		if exclude.from.amount, err = strconv.Atoi(tmp); err != nil {
			return nil, err
		}
	} else {
		exclude.from.amount = 0
	}

	val, ok = conf["to"]
	if ok {
		tmp, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("\"to\" field in excludes of operation %q is not a string", operationName)
		}
		if strings.HasPrefix(tmp, "-") {
			exclude.to.fromGreatest = true
		}
		if exclude.to.amount, err = strconv.Atoi(tmp); err != nil {
			return nil, err
		}
	} else {
		exclude.to = directedRelativPartitionFromTo{true, 0}
	}

	return exclude, nil
}
