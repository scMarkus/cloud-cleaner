package execution

import (
	"log"
	"sync"

	"smartclip.de/cloud-cleaner/config"
	"smartclip.de/cloud-cleaner/types"
)

func StartProviders(conf *config.RuntimeConfig) error {
	var wgProvider sync.WaitGroup

	providerChan := make(chan types.PartitionProvider)
	errChan := make(chan error)

	for i := 0; i < conf.ProviderConcurrency; i++ {
		log.Printf("provider worker start (id: %d)", i)
		go collectPartitions(providerChan, conf.Sources, errChan, &wgProvider)
	}

	go func() {
		for providerName, provider := range conf.Providers {
			wgProvider.Add(1)
			log.Printf("provider %q queued", providerName)
			providerChan <- provider
		}

		log.Printf("started all providers")
		close(providerChan)
	}()

	err := <-errChan
	if err != nil {
		return err
	}

	return nil
}

func collectPartitions(
	providerChan <-chan types.PartitionProvider,
	sources map[string]types.RuntimeResource,
	errChan chan error,
	wgProvider *sync.WaitGroup,
) {
	var wgResource sync.WaitGroup

	for provider := range providerChan {
		for i := 0; i < provider.GetResourceConcurrency(); i++ {
			log.Printf("resource worker start (id: %d) for provider %q", i, provider.GetProviderName())
			go provider.CollectPartitions(errChan, &wgResource)
		}

		for _, resource := range provider.GetRelatedResources() {
			// skip non source resources (they may be empty and give access errors)
			if _, ok := sources[resource.GetResourceName()]; !ok {
				continue
			}

			wgResource.Add(1)
			provider.ResourceInputChan() <- resource
			log.Printf("partition collection for resource %q started", resource.GetResourceName())
		}
		wgResource.Wait()
		log.Printf("partition collection for provider %q finished", provider.GetProviderName())
		wgProvider.Done()
	}

	wgProvider.Wait()
	close(errChan)
}
