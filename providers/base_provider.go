package providers

import (
	"fmt"
	"log"

	"smartclip.de/cloud-cleaner/types"
)

type BaseProvider struct {
	Name         string
	ProviderType types.ProviderType
	InputChan    chan types.RuntimeResource
	Resources    []types.RuntimeResource
	Concurrency  int
}

func (client *BaseProvider) GetProviderName() string {
	return client.Name
}

func (client *BaseProvider) GetProviderType() types.ProviderType {
	return client.ProviderType
}

func (client *BaseProvider) ResourceInputChan() chan<- types.RuntimeResource {
	return client.InputChan
}

func (provider *BaseProvider) GetRelatedResources() []types.RuntimeResource {
	return provider.Resources
}

func (provider *BaseProvider) GetResourceConcurrency() int {
	return provider.Concurrency
}

func MakeBaseProvider(conf map[string]interface{}) (base BaseProvider, err error) {
	var (
		val interface{}
		ok  bool
		tmp string
	)

	if val, ok = conf["name"]; !ok {
		return BaseProvider{}, fmt.Errorf("provider without name field")
	}
	if base.Name, ok = val.(string); !ok {
		return BaseProvider{}, fmt.Errorf("name field of some provider is not of type string")
	}

	if val, ok = conf["kind"]; !ok {
		return BaseProvider{}, fmt.Errorf("provider %q without kind field", base.Name)
	}
	if tmp, ok = val.(string); !ok {
		return BaseProvider{}, fmt.Errorf("kind field of %q is not of type string", base.Name)
	}
	base.ProviderType = types.ProviderType(tmp)
	if _, ok = KnownProviderTypes[base.ProviderType]; !ok {
		return BaseProvider{}, fmt.Errorf("kind (%q) of provider %q is unknown", tmp, base.Name)
	}

	base.InputChan = make(chan types.RuntimeResource)

	if val, ok := conf["resourceconcurrency"]; !ok {
		base.Concurrency = 1
	} else {
		if concurrency, ok := val.(int); !ok || concurrency < 1 {
			log.Printf("strange resource oncurrency value of \"%d\", sanatizing to '1'", concurrency)
			base.Concurrency = 1
		} else {
			base.Concurrency = concurrency
		}
	}

	return
}
