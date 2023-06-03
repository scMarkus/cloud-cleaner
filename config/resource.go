package config

import (
	"bytes"
	"fmt"

	"github.com/spf13/viper"

	"smartclip.de/cloud-cleaner/partitions"
	"smartclip.de/cloud-cleaner/types"
)

func getResources(rawConfig *viper.Viper, providers map[string]types.PartitionProvider) (map[string]types.RuntimeResource, error) {
	var (
		val      interface{}
		str      string
		provider types.PartitionProvider
	)

	resources := make(map[string]types.RuntimeResource)

	if ok := rawConfig.IsSet("resources"); !ok {
		return nil, fmt.Errorf("no resources definition found")
	}
	resourcesList, ok := rawConfig.Get("resources").([]interface{})
	if !ok {
		return nil, fmt.Errorf("resources config is not an array")
	}

	for _, rawResources := range resourcesList {
		resourceSpec, ok := rawResources.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("some resource is not of type map")
		}

		// make sure provider exists
		if val, ok = resourceSpec["provider"]; !ok {
			return nil, fmt.Errorf("some resource has no partition provider configured")
		}
		if str, ok = val.(string); !ok || str == "" {
			return nil, fmt.Errorf("partition provider of some resource is not of type string")
		}
		if provider, ok = providers[str]; !ok {
			return nil, fmt.Errorf("partition provider %q is not configured", str)
		}

		runtimeResource, err := provider.MakeRuntimResource(resourceSpec)
		if err != nil {
			return nil, err
		}

		runtimeResource.SetProvider(provider)

		// ensure name uniqueness
		name := runtimeResource.GetResourceName()
		if _, ok := resources[name]; ok {
			return nil, fmt.Errorf("found duplicate resource name %q", name)
		}
		resources[name] = runtimeResource
	}
	return resources, nil
}

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

func Setup() (runtimeConfig RuntimeConfig, err error) {
	// var rawConfig RawConfig
	var rawConfigBytes []byte
	if err = getRawConfig(&rawConfigBytes); err != nil {
		return
	}

	viper := viper.New()
	viper.BindEnv("ProviderConcurrency", "PROVIDERE_CONCURRENCY")
	viper.SetDefault("ProviderConcurrency", 1)
	viper.BindEnv("S3ListingChunkSize", "S3_LISTING_CHUNCK_SIZE")
	viper.SetDefault("S3ListingChunkSize", int32(1000))

	viper.SetConfigType("json")
	viper.ReadConfig(bytes.NewBuffer(rawConfigBytes))

	runtimeConfig, err = getRuntimeConfig(viper)
	if err != nil {
		return
	}

	return runtimeConfig, nil
}
