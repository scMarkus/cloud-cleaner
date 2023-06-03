package exclude

import (
	"fmt"

	"smartclip.de/cloud-cleaner/types"
)

const (
	UnknowIgnoreType              types.ExcludeType = ""
	AbsoluteTimestampExcludeType                    = "absolute_timestamp"
	AbsolutePartitionExcludeType                    = "absolute_partition"
	CurrentTimestampExcludeType                     = "current_timestamp"
	PartitionTimestampExcludeType                   = "partition_timestamp"
	RelativPartitionExcludeType                     = "relative_partition"
)

type ExcludeTypeFunc func(string, map[string]interface{}) (types.Exclude, error)

var KnownExcludes map[types.ExcludeType]ExcludeTypeFunc = map[types.ExcludeType]ExcludeTypeFunc{
	AbsoluteTimestampExcludeType:  MakeAbsoluteTimestampExclude,
	AbsolutePartitionExcludeType:  nil,
	CurrentTimestampExcludeType:   MakeCurrentTimestampExclude,
	PartitionTimestampExcludeType: MakePartitionTimestampExclude,
	RelativPartitionExcludeType:   MakeRelativPartitionExclude,
}

func MakeExclude(operationName string, conf map[string]interface{}) (types.Exclude, error) {
	var (
		tmp             string
		ok              bool
		val             interface{}
		makeExcludeFunc ExcludeTypeFunc
	)

	if val, ok = conf["kind"]; !ok {
		return nil, fmt.Errorf("ignore spec of operation %q without \"kind\"", operationName)
	}
	if tmp, ok = val.(string); !ok {
		return nil, fmt.Errorf("\"kind\" field of operation %q is not a string", operationName)
	}
	if makeExcludeFunc, ok = KnownExcludes[types.ExcludeType(tmp)]; !ok {
		return nil, fmt.Errorf("\"kind\" field of operation %q is unknown exclude type %q", operationName, tmp)
	}

	exclude, err := makeExcludeFunc(operationName, conf)
	if err != nil {
		return nil, err
	}

	return exclude, nil
}
