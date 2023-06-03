package resources

import (
	"fmt"

	"smartclip.de/cloud-cleaner/partitions"
	"smartclip.de/cloud-cleaner/types"
)

func getPartitionSpec(rawPartitions []interface{}) ([]types.PartitionSpec, error) {
	var (
		val               interface{}
		name, rawDataType string
	)
	if len(rawPartitions) < 1 {
		return nil, fmt.Errorf("there was at laest one resource definition without a partition spec")
	}

	runtimePartitions := make([]types.PartitionSpec, len(rawPartitions))

	for idx, rawPartitionSpec := range rawPartitions {
		rawSpec, ok := rawPartitionSpec.(map[string]interface{})
		if !ok {
			return []types.PartitionSpec{}, fmt.Errorf("partition spec is not a map")
		}

		if val, ok = rawSpec["name"]; !ok {
			return []types.PartitionSpec{}, fmt.Errorf("some partition spec has no \"name\" field")
		}
		if name, ok = val.(string); !ok || name == "" {
			return []types.PartitionSpec{}, fmt.Errorf("some partition spec has non string type \"name\" fild")
		}

		if val, ok = rawSpec["datatype"]; !ok {
			return []types.PartitionSpec{}, fmt.Errorf("some partition spec has no \"datatype\" field")
		}
		if rawDataType, ok = val.(string); !ok {
			return []types.PartitionSpec{}, fmt.Errorf("partition spec %q has non string type \"datatype\" fild", name)
		}

		dataType := types.DataType(rawDataType)
		if _, ok := partitions.KnownDataTypes[dataType]; !ok {
			return []types.PartitionSpec{}, fmt.Errorf("data type %q of column %q is unknown", rawDataType, name)
		}

		runtimePartitions[idx] = types.PartitionSpec{
			Name:     name,
			DataType: dataType,
		}
	}

	return runtimePartitions, nil
}

// do not set provider in here because this information is always accessabke by caller
func MakeBaseRuntimResource(conf map[string]interface{}) (BaseResource, error) {
	var (
		ok        bool
		val       interface{}
		str, name string
	)

	if val, ok = conf["name"]; !ok {
		return BaseResource{}, fmt.Errorf("some resource has no name field")
	}
	if str, ok = val.(string); !ok || str == "" {
		return BaseResource{}, fmt.Errorf("some resource has name field which is not a string")
	}
	name = str

	if val, ok = conf["partitionspec"]; !ok {
		return BaseResource{}, fmt.Errorf("%q has no partition spec", name)
	}
	rawPartitionSpec, ok := val.([]interface{})
	if !ok {
		return BaseResource{}, fmt.Errorf("spartition spec of %q is not an array", name)
	}
	partitionSpec, err := getPartitionSpec(rawPartitionSpec)
	if err != nil {
		return BaseResource{}, err
	}

	return BaseResource{
		Name:          name,
		PartitionSpec: partitionSpec,
		Partitions:    make(map[string]types.Partition),
	}, nil
}
