package config

import (
	"testing"
)

func TestSetup(test *testing.T) {
	type expected struct {
		bucket string
		key    string
	}

	// arrange
	testTabel := []struct {
		name     string
		envs     map[string]string
		jsonnnet string
		expected expected
		err      bool
	}{
		{
			name:     "no prefix partition",
			prefix:   "s3://bucket/key/hallo/welt",
			expected: expected{bucket: "bucket", key: "key/hallo/welt"},
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
