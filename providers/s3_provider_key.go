package providers

import (
	"fmt"
	"sync"
	"time"

	"smartclip.de/cloud-cleaner/partitions"
	"smartclip.de/cloud-cleaner/resources"
	"smartclip.de/cloud-cleaner/types"
)

type S3KeyProvider struct {
	S3Provider
	resources []types.RuntimeResource
}

type s3KeyRuntimeResource struct {
	s3BaseRuntimeResource
	regex string
}

type KeyPartition struct {
	partitions.BasePartition
	Size int64
	ts   time.Time
}

func (partition *KeyPartition) GetTimestamp() (time.Time, error) {
	return partition.ts, nil
}

func (provider *S3KeyProvider) Init(conf map[string]interface{}, errChan chan<- error, wg *sync.WaitGroup) {
	s3Provider, err := newS3Provider(conf)
	if err != nil {
		errChan <- err
	}

	provider.S3Provider = s3Provider
	wg.Done()
}

func (provider *S3KeyProvider) MakeRuntimResource(conf map[string]interface{}) (types.RuntimeResource, error) {
	var err error

	resource := s3KeyRuntimeResource{}
	resource.Provider = provider

	tmp, ok := conf["regex"]
	if !ok {
		return nil, fmt.Errorf("regex not set for resource %q", resource.Name)
	}
	if resource.regex, ok = tmp.(string); !ok {
		return nil, fmt.Errorf("regex for resource %q is not of type string", resource.Name)
	}

	if tmp, ok = conf["prefix"]; !ok {
		return nil, fmt.Errorf("prefix not set for resource %q", resource.Name)
	}
	if resource.prefix, ok = tmp.(string); !ok {
		return nil, fmt.Errorf("prefix of resource %q is not a string", resource.Name)
	}

	resource.BaseResource, err = resources.MakeBaseRuntimResource(conf)
	if err != nil {
		return nil, err
	}

	provider.resources = append(provider.resources, &resource)
	return &resource, nil
}

func (currentPartition *KeyPartition) UpdatePartition(updatePartition types.Partition) error {
	otherPartition, ok := updatePartition.(*KeyPartition)
	if !ok {
		return fmt.Errorf("partition has incorrect type for update")
	}

	currentPartition.Size += otherPartition.Size

	if currentPartition.ts.After(otherPartition.ts) {
		currentPartition.ts = otherPartition.ts
	}

	return nil
}
