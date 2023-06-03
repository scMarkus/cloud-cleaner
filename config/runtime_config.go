package config

import (
	"flag"
	"log"

	"github.com/spf13/viper"

	"smartclip.de/cloud-cleaner/types"
)

type GlobalConfig struct {
	Armed               bool
	ProviderConcurrency int
}

type RuntimeConfig struct {
	Providers  map[string]types.PartitionProvider
	Resources  map[string]types.RuntimeResource
	Operations map[string]types.RuntimeOperationSingle
	Sources    map[string]types.RuntimeResource
	GlobalConfig
}

func getRuntimeConfig(rawConfig *viper.Viper) (conf RuntimeConfig, err error) {
	if conf.Providers, err = getPartitionProviders(rawConfig); err != nil {
		return
	}

	if conf.Resources, err = getResources(rawConfig, conf.Providers); err != nil {
		return
	}

	if conf.Operations, conf.Sources, err = getOperations(rawConfig, conf.Resources); err != nil {
		return
	}

	// force positive Parallelism even if configured differently
	concurrency := rawConfig.GetInt("ProviderConcurrency")
	if concurrency < 1 {
		log.Printf("strange concurrency value of '%d', sanatizing to '1'", concurrency)
		concurrency = 1
	}
	conf.ProviderConcurrency = concurrency

	armed := flag.Bool("armed", false, "activate configured actions (may cause data loss)")
	flag.Parse()

	conf.Armed = *armed
	return
}
