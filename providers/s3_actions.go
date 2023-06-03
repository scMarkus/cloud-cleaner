package providers

import (
	"log"
	"net/url"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"smartclip.de/cloud-cleaner/types"
)

func (provider S3Provider) CopyPartition(
	partititons types.PartitionList,
	source types.RuntimeResource,
	target types.RuntimeResource,
) (types.PreparedActions, error) {
	var (
		wg              sync.WaitGroup
		preparedActions types.PreparedActions
	)

	mutex := &sync.Mutex{}
	errChan := make(chan error)
	sourceResource, _ := source.(S3Resource) // error case handled at resource creation
	targetResource, _ := target.(S3Resource) // error case handled at resource creation
	sourceBucket, sourcePrefix, err := splitBucketAndKey(sourceResource.getPrefix())
	if err != nil {
		return nil, err
	}

	sourcePartitionSpec := source.GetPartitionSpec()
	sourcePartitionKeys := make([]string, len(sourcePartitionSpec))
	for _, partition := range partititons {
		wg.Add(1)
		go func(partition types.Partition) {
			defer wg.Done()
			partitionValues := partition.GetValues()

			for idx, spec := range sourcePartitionSpec {
				sourcePartitionKeys[idx] = spec.Name + "=" + partitionValues[idx]
			}

			sourceKey := sourcePrefix + "/" + strings.Join(sourcePartitionKeys, "/")
			listPrefix := s3.ListObjectsV2Input{
				Bucket:  &sourceBucket,
				Prefix:  &sourceKey,
				MaxKeys: 100, // TODO: make configurable
			}
			listObjectOutput := &s3.ListObjectsV2Output{IsTruncated: true}
			var singleObjectActions []func() error
			for listObjectOutput.IsTruncated {
				if listObjectOutput, err = provider.s3Client.listS3(&listPrefix); err != nil {
					errChan <- err
					return
				}
				listPrefix.ContinuationToken = listObjectOutput.NextContinuationToken

				for _, s3Object := range listObjectOutput.Contents {
					sourceObjectKey := sourceBucket + "/" + *s3Object.Key
					targetPrefix := targetResource.getPrefix() + strings.TrimPrefix(*s3Object.Key, sourcePrefix)
					targetBucket, targetKey, err := splitBucketAndKey(targetPrefix)
					if err != nil {
						errChan <- err
						return
					}

					log.Printf("preparing cp: s3://%s -> s3://%s/%s", sourceObjectKey, targetBucket, targetKey)

					copyInput := &s3.CopyObjectInput{
						CopySource: aws.String(url.PathEscape(sourceObjectKey)),
						Bucket:     aws.String(targetBucket),
						Key:        aws.String(targetKey),
					}
					// TODO: existing files get overwritten -> check why 'check target' not works
					singleObjectActions = append(singleObjectActions, func() error {
						log.Printf("executing cp: s3://%s -> s3://%s/%s", sourceObjectKey, targetBucket, targetKey)
						return provider.s3Client.copy(copyInput)
					})
				}
			}

			action := func() error {
				for _, objectAction := range singleObjectActions {
					if err := objectAction(); err != nil {
						return err
					}
				}

				return nil
			}

			mutex.Lock()
			preparedActions = append(preparedActions, types.PreparedPartitionAction{
				Partition: partition,
				Action:    action,
			})
			mutex.Unlock()
		}(partition)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	if err = <-errChan; err != nil {
		return nil, err
	}

	return preparedActions, nil
}

func (provider S3Provider) RemovePartition(partititons types.PartitionList, source types.RuntimeResource) (types.PreparedActions, error) {
	var preparedActions types.PreparedActions

	sourceResource, _ := source.(S3Resource) // error case handled at resource creation
	sourceBucket, sourcePrefix, err := splitBucketAndKey(sourceResource.getPrefix())
	if err != nil {
		return nil, err
	}

	sourcePartitionSpec := source.GetPartitionSpec()
	sourcePartitionKeys := make([]string, len(sourcePartitionSpec))
	// TODO: implement async partition processing like in copy partitions (don't forget append mutex)
	for _, partition := range partititons {
		partition.GetDependencies()
		partitionValues := partition.GetValues()

		for idx, spec := range sourcePartitionSpec {
			sourcePartitionKeys[idx] = spec.Name + "=" + partitionValues[idx]
		}

		sourceKey := sourcePrefix + "/" + strings.Join(sourcePartitionKeys, "/")
		listPrefix := s3.ListObjectsV2Input{
			Bucket:  &sourceBucket,
			Prefix:  &sourceKey,
			MaxKeys: 100, // TODO: make configurable
		}
		listObjectOutput := &s3.ListObjectsV2Output{IsTruncated: true}

		var singleObjectActions []func() error
		for listObjectOutput.IsTruncated {
			if listObjectOutput, err = provider.s3Client.listS3(&listPrefix); err != nil {
				return nil, err
			}
			listPrefix.ContinuationToken = listObjectOutput.NextContinuationToken

			for _, s3Object := range listObjectOutput.Contents {
				log.Printf("preparing rm: s3://%s", sourceBucket+"/"+*s3Object.Key)

				deleteInput := &s3.DeleteObjectInput{
					Bucket: aws.String(sourceBucket),
					Key:    s3Object.Key,
				}

				singleObjectActions = append(singleObjectActions, func() error {
					log.Printf("executing rm: s3://%s", sourceBucket+"/"+*s3Object.Key)
					return provider.s3Client.delete(deleteInput)
				})
			}
		}

		action := func() error {
			for _, action := range singleObjectActions {
				if err := action(); err != nil {
					return err
				}
			}

			return nil
		}

		preparedActions = append(preparedActions, types.PreparedPartitionAction{
			Partition: partition,
			Action:    action,
		})
	}

	return preparedActions, nil
}
