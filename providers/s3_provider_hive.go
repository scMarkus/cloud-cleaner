package providers

import (
	"fmt"
	"sync"
	"time"

	"smartclip.de/cloud-cleaner/partitions"
	"smartclip.de/cloud-cleaner/resources"
	"smartclip.de/cloud-cleaner/types"
)

type S3HiveProvider struct {
	S3Provider
}

type s3HiveRuntimeResource struct {
	s3BaseRuntimeResource
}

type HivePartition struct {
	partitions.BasePartition
	ObjectCount uint
	Size        int64
	EarliestTs  time.Time
	LatestTs    time.Time
}

func (partition *HivePartition) GetTimestamp() (time.Time, error) {
	return partition.LatestTs, nil
}

func (provider *S3HiveProvider) Init(conf map[string]interface{}, errChan chan<- error, wg *sync.WaitGroup) {
	s3Provider, err := newS3Provider(conf)
	if err != nil {
		errChan <- err
	}

	provider.S3Provider = s3Provider
	wg.Done()
}

func (provider *S3HiveProvider) MakeRuntimResource(conf map[string]interface{}) (types.RuntimeResource, error) {
	var (
		err error
		ok  bool
		tmp interface{}
	)

	resource := s3HiveRuntimeResource{}
	resource.Provider = provider

	resource.BaseResource, err = resources.MakeBaseRuntimResource(conf)
	if err != nil {
		return nil, err
	}

	if tmp, ok = conf["prefix"]; !ok {
		return nil, fmt.Errorf("prefix not set for resource %q", resource.Name)
	}
	if resource.prefix, ok = tmp.(string); !ok {
		return nil, fmt.Errorf("prefix of resource %q is not a string", resource.Name)
	}

	provider.Resources = append(provider.Resources, &resource)
	return &resource, nil
}

func (currentPartition *HivePartition) UpdatePartition(updatePartition types.Partition) error {
	otherPartition, ok := updatePartition.(*HivePartition)
	if !ok {
		return fmt.Errorf("partition has incorrect type for update")
	}

	currentPartition.ObjectCount += otherPartition.ObjectCount
	currentPartition.Size += otherPartition.Size

	if currentPartition.EarliestTs.After(otherPartition.EarliestTs) {
		currentPartition.EarliestTs = otherPartition.EarliestTs
	}
	if currentPartition.LatestTs.Before(otherPartition.LatestTs) {
		currentPartition.LatestTs = otherPartition.LatestTs
	}

	return nil
}
