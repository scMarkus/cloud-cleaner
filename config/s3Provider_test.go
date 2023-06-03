package config

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestClientType(test *testing.T) {
	s3Provider := S3Provider{
		clientType: "s3_hive",
	}

	if s3Provider.ClientType() != "s3_hive" {
		test.Errorf("'s3 client type' failed")
	}
}

func TestResourceInputChan(test *testing.T) {
	resourceChan := make(chan *rawResource)

	s3Provider := S3Provider{
		inputChan: resourceChan,
	}

	if s3Provider.ResourceInputChan() != resourceChan {
		test.Errorf("'s3 resource channel' failed")
	}
}

func TestNewS3HiveProvider(test *testing.T) {
	conf := map[string]string{}
	result, err := NewS3HiveProvider(conf)

	// assert
	if err != nil {
		test.Errorf("'init hive provider' failed with error %q", err)
	}

	if result.ClientType() != "s3_hive" {
		test.Errorf("'init hive provider' failed with %q != %q", result.ClientType(), "s3_hive")
	}
}

func TestNewS3KeyProvider(test *testing.T) {
	conf := map[string]string{}
	result, err := NewS3KeyProvider(conf)

	// assert
	if err != nil {
		test.Errorf("'init key provider' failed with error %q", err)
	}

	if result.ClientType() != "s3_key" {
		test.Errorf("'init key provider' failed with %q != %q", result.ClientType(), "s3_key")
	}
}

func TestNewS3Provider(test *testing.T) {
	conf := map[string]string{}
	result, err := newS3Provider(conf)

	// assert
	if err != nil {
		test.Errorf("'init key provider' failed with error %q", err)
	}

	if result.ClientType() != "" {
		test.Errorf("'init key provider' failed with %#v", result)
	}
}

// this test does not make much sense by itself but it helps coverage
func TestListS3Objects(test *testing.T) {
	s3Provider, _ := newS3Provider(map[string]string{})
	listingBucket := "test"
	filter := s3.ListObjectsV2Input{Bucket: &listingBucket}

	objects, err := s3Provider.s3Client.listS3(&filter)
	if err == nil {
		test.Errorf("'list s3' failed with error %q", err)
	}

	if objects != nil {
		test.Errorf("'list s3' failed with test listing returning %d objects", len(objects.Contents))
	}
}

func TestSplitBucketAndKey(test *testing.T) {
	type expected struct {
		bucket string
		key    string
	}

	// arrange
	testTabel := []struct {
		name     string
		prefix   string
		expected expected
		err      bool
	}{
		{
			name:     "no prefix partition",
			prefix:   "s3://bucket/key/hallo/welt",
			expected: expected{bucket: "bucket", key: "key/hallo/welt"},
		},
		{
			name:     "bucket only",
			prefix:   "s3://bucket",
			expected: expected{bucket: "bucket", key: ""},
		},
		{
			name:   "no s3 protocol error",
			prefix: "bucket/key/hallo/welt",
			err:    true,
		},
		{
			name:     "empty string after protocol",
			prefix:   "s3://",
			expected: expected{bucket: "", key: ""},
			err:      true,
		},
	}

	for _, subtest := range testTabel {
		test.Run(subtest.name, func(t *testing.T) {
			// act
			bucket, key, err := splitBucketAndKey(subtest.prefix)

			// assert
			if (err == nil) == subtest.err {
				test.Errorf("%q failed with unexpected error %q", subtest.name, err)
			}

			if bucket != subtest.expected.bucket || key != subtest.expected.key {
				test.Errorf(
					"%q failed with %v != %v",
					subtest.name,
					expected{bucket: bucket, key: key},
					subtest.expected,
				)
			}
		})
	}
}

func TestMakeHivePartition(test *testing.T) {
	literalPointer := func(s string) *string { return &s }

	// arrange
	testTabel := []struct {
		name     string
		object   types.Object
		expected basePartition
		err      bool
	}{
		{
			name: "happy path",
			object: types.Object{
				Key:          literalPointer("abc=123"),
				LastModified: &time.Time{},
			},
			expected: basePartition{
				partitionValues: []string{"123"},
				ObjectCount:     1,
			},
		},
		{
			name: "object size",
			object: types.Object{
				Key:          literalPointer("abc=123"),
				Size:         2,
				LastModified: &time.Time{},
			},
			expected: basePartition{
				partitionValues: []string{"123"},
				ObjectCount:     1,
				Size:            2,
			},
		},
		{
			name: "longer prefix partition",
			object: types.Object{
				Key:          literalPointer("hallo/world/abc=123"),
				LastModified: &time.Time{},
			},
			expected: basePartition{
				partitionValues: []string{"123"},
				ObjectCount:     1,
			},
		},
		{
			name: "multiple equals in hive partition string",
			object: types.Object{
				Key:          literalPointer("hallo/world/abc=123=test"),
				LastModified: &time.Time{},
			},
			expected: basePartition{
				partitionValues: []string{"123=test"},
				ObjectCount:     1,
			},
		},
		{
			name: "no partition error",
			object: types.Object{
				Key:          literalPointer("hallo/world/abc"),
				LastModified: &time.Time{},
			},
			expected: basePartition{
				partitionValues: []string{},
				ObjectCount:     1,
			},
			err: true,
		},
	}

	for _, subtest := range testTabel {
		test.Run(subtest.name, func(t *testing.T) {
			// act
			partition, err := makeHivePartition(&subtest.object)

			// assert
			if (err == nil) == subtest.err {
				test.Errorf("%q failed with unexpected error %q", subtest.name, err)
			}

			var partitionValuesMatch bool
			for idx, value := range subtest.expected.partitionValues {
				if value != partition.partitionValues[idx] {
					partitionValuesMatch = true
					break
				}
			}

			if partition.ObjectCount != subtest.expected.ObjectCount ||
				partition.Size != subtest.expected.Size ||
				partition.EarliestTs != subtest.expected.EarliestTs ||
				partition.LatestTs != subtest.expected.LatestTs ||
				partitionValuesMatch {
				test.Errorf(
					"%q failed with\nactual: %#v\nexpected: %#v",
					subtest.name,
					partition,
					subtest.expected,
				)
			}
		})
	}
}

func TestHivePartitioning(test *testing.T) {
	literalPointer := func(s string) *string { return &s }

	// arrange
	testTabel := []struct {
		name     string
		object   types.Object
		resource rawResource
		expected basePartition
		err      bool
	}{
		{
			name: "happy path",
			object: types.Object{
				Key:          literalPointer("abc=123"),
				LastModified: &time.Time{},
			},
			resource: rawResource{
				Partitions: make(map[string]*basePartition),
			},
			expected: basePartition{
				partitionValues: []string{"123"},
				ObjectCount:     1,
				EarliestTs:      time.Time{},
				LatestTs:        time.Time{},
			},
		},
		{
			name: "no partition error",
			object: types.Object{
				Key:          literalPointer("hallo/world/abc"),
				LastModified: &time.Time{},
			},
			resource: rawResource{
				Partitions: make(map[string]*basePartition),
			},
			expected: basePartition{
				partitionValues: []string{},
				ObjectCount:     1,
				EarliestTs:      time.Time{},
				LatestTs:        time.Time{},
			},
			err: true,
		},
	}

	for _, subtest := range testTabel {
		test.Run(subtest.name, func(t *testing.T) {
			errorChan := make(chan error, 1)

			// act
			var latestPartition string
			hivePartitioning(&subtest.resource, &subtest.object, &latestPartition, errorChan)

			// assert
			select {
			case err := <-errorChan:
				if !subtest.err {
					test.Errorf("%q failed with unexpected error %q", subtest.name, err)
				}
			default: // pass if no error
			}

			tmpPartitions := subtest.resource.Partitions
			if len(tmpPartitions) > 1 {
				test.Errorf("%q failed because having more then one partition to check ", subtest.name)
			}

			for _, partition := range subtest.resource.Partitions {
				var partitionValuesMatch bool
				for idx, value := range subtest.expected.partitionValues {
					if value != partition.PartitionValues[idx] {
						partitionValuesMatch = true
						break
					}
				}

				if partition.ObjectCount != subtest.expected.ObjectCount ||
					partition.Size != subtest.expected.Size ||
					partition.EarliestTs != subtest.expected.EarliestTs ||
					partition.LatestTs != subtest.expected.LatestTs ||
					partitionValuesMatch {
					test.Errorf(
						"%q failed with\nactual: %#v\nexpected: %#v",
						subtest.name,
						partition,
						subtest.expected,
					)
				}
			}
		})
	}
}

func TestKeysPartitioning(test *testing.T) {
	literalPointer := func(s string) *string { return &s }

	// arrange
	testTabel := []struct {
		name     string
		object   types.Object
		resource rawResource
		expected basePartition
		err      bool
	}{
		{
			name:   "happy path",
			object: types.Object{Key: literalPointer("abc/test_1.txt"), LastModified: &time.Time{}},
			resource: rawResource{
				PartitionSpec: []PartitionSpec{{Name: "a", DataType: "string"}},
				Regex:         `.+/test_(\d+).txt`,
				Partitions:    make(map[string]*basePartition),
			},
			expected: basePartition{
				partitionValues: []string{"1"},
				ObjectCount:     1,
				EarliestTs:      time.Time{},
				LatestTs:        time.Time{},
			},
		},
		{
			name:   "single matching group but 2 columns ",
			object: types.Object{Key: literalPointer("abc/test_1.txt"), LastModified: &time.Time{}},
			resource: rawResource{
				PartitionSpec: []PartitionSpec{{Name: "a", DataType: "int"}, {Name: "b", DataType: "int"}},
				Regex:         `.+/test_(\d+).txt`,
				Partitions:    make(map[string]*basePartition),
			},
			expected: basePartition{
				partitionValues: []string{"1"},
				ObjectCount:     1,
				EarliestTs:      time.Time{},
				LatestTs:        time.Time{},
			},
			err: true,
		},
		{
			name:   "capture group empty match",
			object: types.Object{Key: literalPointer("abc/test_1.txt"), LastModified: &time.Time{}},
			resource: rawResource{
				PartitionSpec: []PartitionSpec{{Name: "a", DataType: "int"}},
				Regex:         `.+/test_\d+(.*).txt`,
				Partitions:    make(map[string]*basePartition),
			},
			expected: basePartition{
				partitionValues: []string{""},
				ObjectCount:     1,
				EarliestTs:      time.Time{},
				LatestTs:        time.Time{},
			},
			err: true,
		},
		{
			name:   "duplicate partition key",
			object: types.Object{Key: literalPointer("abc/test_1.txt"), LastModified: &time.Time{}},
			resource: rawResource{
				PartitionSpec: []PartitionSpec{{Name: "a", DataType: "int"}},
				Regex:         `.+/test_(\d+).txt`,
				Partitions: map[string]*basePartition{"1": {
					partitionValues: []string{"1"},
					ObjectCount:     1,
					EarliestTs:      time.Time{},
					LatestTs:        time.Time{},
				}},
			},
			expected: basePartition{
				partitionValues: []string{"1"},
				ObjectCount:     1,
				EarliestTs:      time.Time{},
				LatestTs:        time.Time{},
			},
			err: true,
		},
	}

	for _, subtest := range testTabel {
		test.Run(subtest.name, func(t *testing.T) {
			errorChan := make(chan error, 1)

			// act
			var latestPartitino string
			keysPartitioning(&subtest.resource, &subtest.object, &latestPartitino, errorChan)

			// assert
			select {
			case err := <-errorChan:
				if !subtest.err {
					test.Errorf("%q failed with unexpected error %q", subtest.name, err)
				}
			default: // pass if no error
			}

			tmpPartitions := subtest.resource.Partitions
			if len(tmpPartitions) > 1 {
				test.Errorf("%q failed because having more then one resource to check ", subtest.name)
			}

			for _, partition := range subtest.resource.Partitions {
				var partitionValuesMatch bool
				for idx, value := range subtest.expected.partitionValues {
					if value != partition.PartitionValues[idx] {
						partitionValuesMatch = true
						break
					}
				}

				if partition.ObjectCount != subtest.expected.ObjectCount ||
					partition.Size != subtest.expected.Size ||
					partition.EarliestTs != subtest.expected.EarliestTs ||
					partition.LatestTs != subtest.expected.LatestTs ||
					partitionValuesMatch {
					test.Errorf(
						"%q failed with\nactual: %#v\nexpected: %#v",
						subtest.name,
						partition,
						subtest.expected,
					)
				}
			}
		})
	}
}

type mockingFunc func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
type mockClient struct {
	mockingFunc mockingFunc
}

func (client mockClient) listS3(s3ObjectFilter *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return client.mockingFunc(s3ObjectFilter)
}

func TestCollectS3Partitions(test *testing.T) {
	literalPtr := func(s string) *string { return &s }

	var idx int

	objects := []s3.ListObjectsV2Output{
		{IsTruncated: true, Contents: []types.Object{{Key: literalPtr("abc=123"), LastModified: &time.Time{}}}},
		{Contents: []types.Object{{Key: literalPtr("abc=123"), LastModified: &time.Time{}}}},
	}

	// arrange
	testTabel := []struct {
		name     string
		s3Mock   mockingFunc
		resource rawResource
		expected basePartition
		err      bool
	}{
		{
			name: "multiple chunks",
			s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
				return func() (result *s3.ListObjectsV2Output, err error) {
					result, err = &objects[idx], nil
					idx++
					return
				}()
			},
			resource: rawResource{S3ListingChunkSize: 1, Prefix: "s3://abc", PartitionProvider: rawProvider{Name: "s3_hive"}},
			expected: basePartition{partitionValues: []string{"123"}, ObjectCount: 2, EarliestTs: time.Time{}, LatestTs: time.Time{}},
		},
		{
			name: "happy path",
			s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{
					Contents: []types.Object{{Key: literalPtr("abc=123"), LastModified: &time.Time{}}},
				}, nil
			},
			resource: rawResource{Prefix: "s3://abc", PartitionProvider: rawProvider{Name: "s3_hive"}},
			expected: basePartition{partitionValues: []string{"123"}, ObjectCount: 1, EarliestTs: time.Time{}, LatestTs: time.Time{}},
		},
		{
			name: "no s3 prefix",
			s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{Contents: []types.Object{{}}}, fmt.Errorf("")
			},
			resource: rawResource{Prefix: "s3a://abc", PartitionProvider: rawProvider{Name: "s3_hive"}},
			err:      true,
		},
		{
			name: "listing error",
			s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{Contents: []types.Object{{}}}, fmt.Errorf("")
			},
			resource: rawResource{Prefix: "s3://abc", PartitionProvider: rawProvider{Name: "s3_hive"}},
			err:      true,
		},
		{
			name: "listing returned no objects",
			s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{Contents: []types.Object{}}, nil
			},
			resource: rawResource{Prefix: "s3://abc", PartitionProvider: rawProvider{Name: "s3_hive"}},
			err:      true,
		},
		{
			name: "contains delta log",
			s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{
					Contents: []types.Object{{Key: literalPtr("_delta_log/"), LastModified: &time.Time{}}},
				}, nil
			},
			resource: rawResource{Prefix: "s3://abc", PartitionProvider: rawProvider{Name: "s3_hive"}},
			err:      true,
		},
		{
			name: "unknown provider",
			s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{
					Contents: []types.Object{{Key: literalPtr("abc=123"), LastModified: &time.Time{}}},
				}, nil
			},
			resource: rawResource{Prefix: "s3://abc", PartitionProvider: rawProvider{Name: "no_s3"}},
			expected: basePartition{partitionValues: []string{"123"}, ObjectCount: 1, EarliestTs: time.Time{}, LatestTs: time.Time{}},
			err:      true,
		},
		{
			name: "regex provider",
			s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{
					Contents: []types.Object{{Key: literalPtr("test_1.txt"), LastModified: &time.Time{}}},
				}, nil
			},
			resource: rawResource{Prefix: "s3://abc", Regex: `(\d+).txt$`,
				PartitionProvider: rawProvider{Name: "s3_key"},
				PartitionSpec:     []PartitionSpec{{Name: "number", DataType: "integer"}},
			},
			expected: basePartition{partitionValues: []string{"1"}, ObjectCount: 1, EarliestTs: time.Time{}, LatestTs: time.Time{}},
		},

		//{
		//	name: "multiple objects in partition",
		//	s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
		//		return &s3.ListObjectsV2Output{
		//			Contents: []types.Object{
		//				{Key: literalPtr("abc=123"), LastModified: &time.Time{}},
		//				{Key: literalPtr("abc=123"), LastModified: &time.Time{}},
		//			},
		//		}, nil
		//	},
		//	resource: Resource{Prefix: "s3://abc", PartitionProvider: Provider{Name: "s3_hive"}},
		//	expected: Partition{PartitionValues: []string{"123"}, ObjectCount: 2, EarliestTs: time.Time{}, LatestTs: time.Time{}},
		//},
		//{
		//	name: "increase partition end",
		//	s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
		//		return &s3.ListObjectsV2Output{
		//			Contents: []types.Object{
		//				{Key: literalPtr("abc=123"), LastModified: timePtr(time.Date(1, 0, 0, 0, 0, 0, 0, time.UTC))},
		//				{Key: literalPtr("abc=123"), LastModified: timePtr(time.Date(1, 1, 0, 0, 0, 0, 0, time.UTC))},
		//			},
		//		}, nil
		//	},
		//	resource: Resource{Prefix: "s3://abc", PartitionProvider: Provider{Name: "s3_hive"}},
		//	expected: Partition{PartitionValues: []string{"123"}, ObjectCount: 2,
		//		EarliestTs: time.Date(1, 0, 0, 0, 0, 0, 0, time.UTC),
		//		LatestTs:   time.Date(1, 1, 0, 0, 0, 0, 0, time.UTC)},
		//},
		//{
		//	name: "decrease partition start",
		//	s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
		//		return &s3.ListObjectsV2Output{
		//			Contents: []types.Object{
		//				{Key: literalPtr("abc=123"), LastModified: timePtr(time.Date(1, 1, 0, 0, 0, 0, 0, time.UTC))},
		//				{Key: literalPtr("abc=123"), LastModified: timePtr(time.Date(1, 0, 0, 0, 0, 0, 0, time.UTC))},
		//			},
		//		}, nil
		//	},
		//	resource: Resource{Prefix: "s3://abc", PartitionProvider: Provider{Name: "s3_hive"}},
		//	expected: Partition{PartitionValues: []string{"123"}, ObjectCount: 2,
		//		EarliestTs: time.Date(1, 0, 0, 0, 0, 0, 0, time.UTC),
		//		LatestTs:   time.Date(1, 1, 0, 0, 0, 0, 0, time.UTC)},
		//},
		//{
		//	name: "increase and decrease partition time",
		//	s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
		//		return &s3.ListObjectsV2Output{
		//			Contents: []types.Object{
		//				{Key: literalPtr("abc=123"), LastModified: timePtr(time.Date(1, 1, 0, 0, 0, 0, 0, time.UTC))},
		//				{Key: literalPtr("abc=123"), LastModified: timePtr(time.Date(1, 0, 0, 0, 0, 0, 0, time.UTC))},
		//				{Key: literalPtr("abc=123"), LastModified: timePtr(time.Date(1, 2, 0, 0, 0, 0, 0, time.UTC))},
		//			},
		//		}, nil
		//	},
		//	resource: Resource{Prefix: "s3://abc", PartitionProvider: Provider{Name: "s3_hive"}},
		//	expected: Partition{PartitionValues: []string{"123"}, ObjectCount: 3,
		//		EarliestTs: time.Date(1, 0, 0, 0, 0, 0, 0, time.UTC),
		//		LatestTs:   time.Date(1, 2, 0, 0, 0, 0, 0, time.UTC)},
		//},
		//{
		//	name: "increase object count without time change",
		//	s3Mock: func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
		//		return &s3.ListObjectsV2Output{
		//			Contents: []types.Object{
		//				{Key: literalPtr("abc=123"), LastModified: timePtr(time.Date(1, 0, 0, 0, 0, 0, 0, time.UTC))},
		//				{Key: literalPtr("abc=123"), LastModified: timePtr(time.Date(1, 0, 0, 0, 0, 0, 0, time.UTC))},
		//			},
		//		}, nil
		//	},
		//	resource: Resource{Prefix: "s3://abc", PartitionProvider: Provider{Name: "s3_hive"}},
		//	expected: Partition{PartitionValues: []string{"123"}, ObjectCount: 2,
		//		EarliestTs: time.Date(1, 0, 0, 0, 0, 0, 0, time.UTC),
		//		LatestTs:   time.Date(1, 0, 0, 0, 0, 0, 0, time.UTC)},
		//},
	}

	for _, subtest := range testTabel {
		test.Run(subtest.name, func(t *testing.T) {
			var err error
			var wg sync.WaitGroup
			var wgContinue bool
			errorChan := make(chan error, 1)
			inputChan := make(chan *rawResource)

			provider := S3Provider{
				inputChan: inputChan,
				s3Client:  mockClient{mockingFunc: subtest.s3Mock},
			}

			if subtest.resource.Partitions == nil {
				subtest.resource.Partitions = make(map[string]*basePartition)
			}

			// act
			go provider.CollectPartitions(errorChan, &wg)
			go func() {
				err = <-errorChan
				if !wgContinue {
					wg.Done()
				}
			}()

			// if error is expected always wait for it too
			if subtest.err {
				wg.Add(2)
				wgContinue = false
			} else {
				wg.Add(1)
				wgContinue = true
			}
			inputChan <- &subtest.resource

			// assert
			wg.Wait()

			if (err == nil) == subtest.err {
				test.Errorf("%q failed with unexpected error %q", subtest.name, err)
			}

			tmpPartitions := subtest.resource.Partitions
			if len(tmpPartitions) > 1 {
				test.Errorf("%q failed because having more then one partition to check ", subtest.name)
			}

			for _, partition := range subtest.resource.Partitions {
				var partitionValuesMatch bool
				for idx, value := range subtest.expected.partitionValues {
					if value != partition.PartitionValues[idx] {
						partitionValuesMatch = true
						break
					}
				}

				if partition.ObjectCount != subtest.expected.ObjectCount ||
					partition.Size != subtest.expected.Size ||
					partition.EarliestTs != subtest.expected.EarliestTs ||
					partition.LatestTs != subtest.expected.LatestTs ||
					partitionValuesMatch {
					test.Errorf(
						"%q failed with\nactual: %#v\nexpected: %#v",
						subtest.name,
						partition,
						subtest.expected,
					)
				}
			}
		})
	}
}

//func TestHashCompatibleEncoding(test *testing.T) {
//	testTabel := []struct {
//		name     string
//		input    Partition
//		expected string
//	}{
//		{
//			name:     "partition encoding",
//			input:    Partition{PartitionValues: []string{"abc", "123"}},
//			expected: "abc\t123",
//		},
//	}
//	for _, subtest := range testTabel {
//		test.Run(subtest.name, func(t *testing.T) {
//			// act
//			result := subtest.input.HashCompatibleEncoding()
//
//			// assert
//			if result != subtest.expected {
//				test.Errorf("%q failed with %q != %q", subtest.name, result, subtest.expected)
//			}
//		})
//	}
//}
