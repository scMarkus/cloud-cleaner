package config

import (
	"fmt"
	"log"
	"sync"

	"github.com/spf13/viper"

	pp "smartclip.de/cloud-cleaner/providers"
	"smartclip.de/cloud-cleaner/types"
)

func getPartitionProviders(conf *viper.Viper) (map[string]types.PartitionProvider, error) {
	var (
		providerTemplate types.PartitionProvider
		wg               sync.WaitGroup
	)

	initErrChan := make(chan error)
	providers := make(map[string]types.PartitionProvider)

	if ok := conf.IsSet("providers"); !ok {
		return nil, fmt.Errorf("no providers definition found")
	}
	providersList, ok := conf.Get("providers").([]interface{})
	if !ok {
		return nil, fmt.Errorf("providers config is not an array")
	}

	for _, rawProvider := range providersList {
		providerSpec := rawProvider.(map[string]interface{})
		baseProvider, err := pp.MakeBaseProvider(providerSpec)
		if err != nil {
			return nil, err
		}

		// get uniitilized provider templates of configured provider type
		switch baseProvider.ProviderType {
		case pp.S3HiveProviderType:
			providerTemplate = &pp.S3HiveProvider{}
		case pp.S3KeyProviderType:
			providerTemplate = &pp.S3KeyProvider{}
		case pp.TrinoProviderType:
			providerTemplate = &pp.TrinoClient{}
		default:
			return nil, fmt.Errorf("unrecognized partition provider type %q for %q", baseProvider.ProviderType, baseProvider.Name)
		}

		// get config and initialize the provider
		wg.Add(1)
		go providerTemplate.Init(providerSpec, initErrChan, &wg)

		// store provider for later reference
		providers[baseProvider.Name] = providerTemplate
	}

	// wait for all providers to be initialized and then close corresponding channel
	go func() {
		defer close(initErrChan)
		wg.Wait()
	}()
	// this will either unblock because of some error or because channel close (all completed)
	if err := <-initErrChan; err != nil {
		return nil, err
	}

	// check provider access is functioning
	checkErrChan := make(chan error)
	for _, provider := range providers {
		wg.Add(1)
		go provider.CheckAccess(checkErrChan, &wg)
	}
	// wait for access checks
	go func() {
		defer close(checkErrChan)

		wg.Wait()
	}()
	if err := <-checkErrChan; err != nil {
		return nil, err
	}

	return providers, nil
}

func commonProviderAttributes(provider *pp.BaseProvider, conf map[string]interface{}) {
	if val, ok := conf["resourceconcurrency"]; !ok {
		provider.Concurrency = 1
	} else {
		if concurrency, ok := val.(int); !ok || concurrency < 1 {
			log.Printf("strange resource oncurrency value of \"%d\", sanatizing to '1'", concurrency)
			provider.Concurrency = 1
		} else {
			provider.Concurrency = concurrency
		}
	}
}
