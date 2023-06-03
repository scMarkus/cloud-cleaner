package providers

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"smartclip.de/cloud-cleaner/resources"
	"smartclip.de/cloud-cleaner/types"
)

type S3Resource interface {
	types.RuntimeResource
	getPrefix() string
}

type s3BaseRuntimeResource struct {
	resources.BaseResource
	prefix string
}

func (resource s3BaseRuntimeResource) getPrefix() string {
	return resource.prefix
}

// used for mocking
type s3Client interface {
	listS3(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
	copy(*s3.CopyObjectInput) error
	delete(*s3.DeleteObjectInput) error
	buckets() (*s3.ListBucketsOutput, error)
}

// minimal s3 client wrapper for easier mocking
type s3ListingClient struct {
	*s3.Client
}

func (client s3ListingClient) listS3(s3ObjectFilter *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return client.ListObjectsV2(context.TODO(), s3ObjectFilter)
}

func (client s3ListingClient) copy(input *s3.CopyObjectInput) error {
	_, err := client.CopyObject(context.TODO(), input)
	return err
}

func (client s3ListingClient) delete(input *s3.DeleteObjectInput) error {
	_, err := client.DeleteObject(context.TODO(), input)
	return err
}

func (client s3ListingClient) buckets() (*s3.ListBucketsOutput, error) {
	return client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
}

type S3Provider struct {
	BaseProvider
	s3Client s3Client
}
