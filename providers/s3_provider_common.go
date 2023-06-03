package providers

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func newS3Provider(conf map[string]interface{}) (provider S3Provider, err error) {
	// configure via envs 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY' and 'AWS_DEFAULT_REGION'

	if provider.BaseProvider, err = MakeBaseProvider(conf); err != nil {
		return
	}

	clientConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return S3Provider{}, err
	}
	provider.s3Client = s3ListingClient{s3.NewFromConfig(clientConfig)}

	return
}

func (provider *S3Provider) CheckAccess(errChan chan<- error, wg *sync.WaitGroup) {
	_, err := provider.s3Client.buckets()
	if err != nil {
		errChan <- err
	}

	wg.Done()
}

func (provider S3Provider) CollectPartitions(errorChannel chan<- error, wg *sync.WaitGroup) {
	for resourceTmp := range provider.InputChan {
		resource := resourceTmp.(S3Resource)

		bucket, prefix, err := splitBucketAndKey(resource.getPrefix())
		if err != nil {
			errorChannel <- err
			break
		}

		var latestPartitionKey string
		listingChunckNumber := 1
		s3ObjectFilter := &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(prefix),
			MaxKeys: 100, // TODO make available from config
		}
		listObjectOutput := &s3.ListObjectsV2Output{IsTruncated: true}

		// s3 objects are loaded in chunks -> iterate over all chunks
		log.Printf("start s3 partition collection for %q", resource.GetResourceName())
		for listObjectOutput.IsTruncated {
			if listObjectOutput, err = provider.s3Client.listS3(s3ObjectFilter); err != nil {
				log.Printf("s3 listing error... your s3 prefix may not exist (%q)", "s3://"+bucket+"/"+prefix)
				errorChannel <- err
				return
			}

			if len(listObjectOutput.Contents) < 1 {
				errorChannel <- fmt.Errorf("s3 listing of %q returned no objects", "s3://"+bucket+"/"+prefix)
				return
			}

			// only keep needed prefix information and append to list
			for _, s3Object := range listObjectOutput.Contents {
				if strings.Contains(*s3Object.Key, "2023-06") {
					listingChunckNumber++
				}

				// TODO: this likely only need to be checked for the first element?
				if strings.Contains(*s3Object.Key, "_delta_log/") {
					errorChannel <- fmt.Errorf("the key %q, indicates a delta lake table (avoid managing yourself)", *s3Object.Key)
					return
				}

				switch r := resource.(type) {
				case *s3HiveRuntimeResource:
					hivePartitioning(r, &s3Object, &latestPartitionKey, errorChannel)
				case *s3KeyRuntimeResource:
					keysPartitioning(r, &s3Object, &latestPartitionKey, errorChannel)
				default:
					errorChannel <- fmt.Errorf("s3 resource type %q unknown", resource.GetResourceName())
					return
				}
			}

			// log some debugging information
			log.Printf(
				"chunk %d of resource %q found %d partitions so far",
				listingChunckNumber,
				resource.GetResourceName(),
				len(resource.GetPartitions()),
			)

			// update ContinuationToken so next chunk will be loaded
			s3ObjectFilter.ContinuationToken = listObjectOutput.NextContinuationToken

			listingChunckNumber++
		}

		log.Printf("finished s3 partition collection for %q", resource.GetResourceName())
		wg.Done()
	}
}
