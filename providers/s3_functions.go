package providers

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"smartclip.de/cloud-cleaner/partitions"
)

func splitBucketAndKey(prefix string) (bucket string, key string, err error) {
	if !strings.HasPrefix(prefix, "s3://") {
		err = fmt.Errorf("s3 prefix string does not start with 's3://'")
		return
	}

	parts := strings.SplitN(prefix[5:], "/", 2)
	switch len(parts) {
	case 2:
		bucket = parts[0]
		key = parts[1]
	case 1:
		bucket = parts[0]
	}

	if len(bucket) < 1 && len(key) < 1 {
		err = fmt.Errorf("bucket and key could not be set for %q", prefix)
	}

	return
}

func hivePartitioning(resource *s3HiveRuntimeResource, s3Object *s3Types.Object, latestPartition *string, errChan chan<- error) {
	partitionSpecIdx := 0
	partitionSpec := resource.GetPartitionSpec()

	// for large lists this probably puts many object on the heap -> much GC
	newObjectPartition := HivePartition{
		ObjectCount: 1,
		Size:        s3Object.Size,
		EarliestTs:  *s3Object.LastModified,
		LatestTs:    *s3Object.LastModified,
		BasePartition: partitions.BasePartition{
			Resource:        resource,
			PartitionValues: make([]string, len(partitionSpec)),
			CompletionWg:    &sync.WaitGroup{},
		},
	}

	// split on '/' which is s3 separator and
	// see if any element contains '=' which would make it a hive partition
	for _, element := range strings.Split(*s3Object.Key, "/") {
		partitionKeyValue := strings.SplitN(element, "=", 2)
		// if there are 2 elements in the list it is a hive partition
		if len(partitionKeyValue) == 2 {
			if partitionSpec[partitionSpecIdx].Name != partitionKeyValue[0] {
				errChan <- fmt.Errorf("resource %q encountered not matching partition key in %q", resource.Name, *s3Object.Key)
			}

			// hive partition can contain multiple columns
			newObjectPartition.PartitionValues[partitionSpecIdx] = partitionKeyValue[1]
			partitionSpecIdx++
		}
	}
	if len(newObjectPartition.PartitionValues) < 1 {
		errChan <- fmt.Errorf("%q does not contain hive partition", *s3Object.Key)
	}

	parsedValues, err := partitions.ParsePartitionString(resource.PartitionSpec, newObjectPartition.PartitionValues)
	if err != nil {
		errChan <- err
		return
	}
	newObjectPartition.TypedPartitionValues = parsedValues

	// only append new values (there may be many files within same partition)
	resource.IncorporatePartition(&newObjectPartition)
	*latestPartition = newObjectPartition.TypedPartitionValues.ToString()
}

func keysPartitioning(resource *s3KeyRuntimeResource, s3Object *s3Types.Object, latestPartition *string, errorChannel chan<- error) {
	// I have looked into how errors are generated and handling or creating tests for them
	// would just be to much efort. when you hit one of those you are likely having other issues
	regex, _ := regexp.Compile(resource.regex)

	allMatches := regex.FindStringSubmatch(*s3Object.Key)
	matches := allMatches[1:] // expected to always have full string in first entry and subgroubs after
	if len(matches) != len(resource.Partitions) {
		errorChannel <- fmt.Errorf("mismatching regex captcher group count with partition spec column count for resource %q", resource.Name)
	}
	for idx, capture := range matches {
		if capture == "" {
			errorChannel <- fmt.Errorf("capture group %d of s3 object key %q is empty", idx, *s3Object.Key)
		}
	}

	newPartition := KeyPartition{
		BasePartition: partitions.BasePartition{
			PartitionValues: matches,
			Resource:        resource,
			CompletionWg:    &sync.WaitGroup{},
		},
		Size: s3Object.Size,
		ts:   *s3Object.LastModified,
	}

	parsedValues, err := partitions.ParsePartitionString(resource.PartitionSpec, newPartition.PartitionValues)
	if err != nil {
		errorChannel <- err
		return
	}
	newPartition.TypedPartitionValues = parsedValues

	*latestPartition = newPartition.TypedPartitionValues.ToString()

	if _, ok := resource.Partitions[*latestPartition]; ok {
		errorChannel <- fmt.Errorf("for regex provided partitions they are expected to be uniq but %v was not", matches)
	} else {
		resource.Partitions[*latestPartition] = &newPartition
	}
}
