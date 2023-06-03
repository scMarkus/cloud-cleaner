package partitions

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"smartclip.de/cloud-cleaner/types"
)

const (
	UnknownDataType types.DataType = ""
	Date                           = "date"
	DateTime                       = "datetime"
	Time                           = "time"
	Int                            = "int"
	String                         = "string"
)

var KnownDataTypes map[types.DataType]types.PartitionValueParsing = map[types.DataType]types.PartitionValueParsing{
	Date:     ParseDatePartition,
	DateTime: ParseDateTimePartition,
	Time:     ParseTimePartition,
	Int:      ParseIntPartition,
	String:   ParseStringPartition,
}

func ParseIntPartition(str string) (types.TypedPartitionValue, error) {
	number, err := strconv.Atoi(str)
	return intValue{number}, err
}

func ParseStringPartition(str string) (types.TypedPartitionValue, error) {
	return stringValue{str}, nil
}

func ParseDatePartition(str string) (types.TypedPartitionValue, error) {
	currentDate, err := time.Parse("2006-01-02", str)
	if err != nil {
		return nil, err
	}

	return dateValue{currentDate}, nil
}

func ParseDateTimePartition(str string) (types.TypedPartitionValue, error) {
	// in case of s3 hive partitioning the str is url encoded
	decoded, err := url.QueryUnescape(str)
	if err != nil {
		return nil, err
	}

	currentDateTime, err := time.Parse("2006-01-02 15:04:05", decoded)
	if err != nil {
		return nil, err
	}

	return dateTimeValue{currentDateTime}, nil
}

func ParseTimePartition(str string) (types.TypedPartitionValue, error) {
	// in case of s3 hive partitioning the str is url encoded
	decoded, err := url.QueryUnescape(str)
	if err != nil {
		return nil, err
	}

	currentTime, err := time.Parse("15:04:05", decoded)
	if err != nil {
		return nil, err
	}

	return dateTimeValue{currentTime}, nil
}

func ParsePartitionString(specs []types.PartitionSpec, partition []string) (types.TypedPartitionValueList, error) {
	if len(specs) != len(partition) {
		return nil, fmt.Errorf("spec and raw strings do not match for partition value parsing")
	}

	parsedValues := make(types.TypedPartitionValueList, len(specs))
	for idx, rawPartitionValue := range partition {
		parsingFunc := KnownDataTypes[specs[idx].DataType]

		parsedVal, err := parsingFunc(rawPartitionValue)
		if err != nil {
			return nil, err
		}
		parsedValues[idx] = parsedVal
	}

	return parsedValues, nil
}
