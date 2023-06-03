package execution

import (
	"testing"
	"time"
)

func TestSameColumn(test *testing.T) {
	// arrange
	testTabel := []struct {
		name     string
		input1   S3PartitionColumn
		input2   S3PartitionColumn
		expected bool
	}{
		{
			name: "matching columns",
			input1: S3PartitionColumn{
				Key:   "hallo",
				Value: "world",
			},
			input2: S3PartitionColumn{
				Key:   "hallo",
				Value: "world",
			},
			expected: true,
		},
		{
			name: "not matching value",
			input1: S3PartitionColumn{
				Key:   "hallo",
				Value: "world",
			},
			input2: S3PartitionColumn{
				Key:   "hallo",
				Value: "test",
			},
			expected: false,
		},
		{
			name: "not matching key",
			input1: S3PartitionColumn{
				Key:   "hallo",
				Value: "world",
			},
			input2: S3PartitionColumn{
				Key:   "test",
				Value: "world",
			},
			expected: false,
		},
		{
			name: "not matching both",
			input1: S3PartitionColumn{
				Key:   "hallo",
				Value: "world",
			},
			input2: S3PartitionColumn{
				Key:   "test",
				Value: "test",
			},
			expected: false,
		},
	}

	for _, subtest := range testTabel {
		test.Run(subtest.name, func(t *testing.T) {
			// act
			result := subtest.input1.sameColumn(&subtest.input2)

			// assert
			if result != subtest.expected {
				test.Errorf("test %q failed for %t != %t", subtest.name, result, subtest.expected)
			}
		})
	}
}

func TestHivePartitionString(test *testing.T) {
	// arrange
	testTabel := []struct {
		name     string
		input    S3PartitionColumn
		expected string
	}{
		{
			name: "convert back to hive style partition",
			input: S3PartitionColumn{
				Key:   "hallo",
				Value: "world",
			},
			expected: "hallo=world",
		},
	}

	for _, subtest := range testTabel {
		test.Run(subtest.name, func(t *testing.T) {
			// act
			result := subtest.input.hivePartitionString()

			// assert
			if result != subtest.expected {
				test.Errorf("test %q failed for %q != %q", subtest.name, result, subtest.expected)
			}
		})
	}
}

func TestUpdateTime(test *testing.T) {
	// arrange
	testTabel := []struct {
		name     string
		input1   S3PartitionColumn
		input2   S3PartitionColumn
		expected S3PartitionColumn
	}{
		{
			name: "all the same",
			input1: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
			},
			input2: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
			},
			expected: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "increase latest_ts",
			input1: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 2, 0, 0, 0, 0, time.UTC),
			},
			input2: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 2, 1, 0, 0, 0, time.UTC),
			},
			expected: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 2, 1, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "decrease earlieast_ts",
			input1: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 2, 0, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 2, 0, 0, 0, 0, time.UTC),
			},
			input2: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 1, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 2, 0, 0, 0, 0, time.UTC),
			},
			expected: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 1, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 2, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "change booth at same time",
			input1: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 2, 0, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 2, 0, 0, 0, 0, time.UTC),
			},
			input2: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 1, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 2, 1, 0, 0, 0, time.UTC),
			},
			expected: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 1, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 2, 1, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "keep latest_ts and earliest_ts",
			input1: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 2, 0, 0, 0, 0, time.UTC),
			},
			input2: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 1, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 1, 1, 0, 0, 0, time.UTC),
			},
			expected: S3PartitionColumn{
				EarliestTs: time.Date(1, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
				LatestTs:   time.Date(1, time.Month(1), 2, 0, 0, 0, 0, time.UTC),
			},
		},
	}

	for _, subtest := range testTabel {
		test.Run(subtest.name, func(t *testing.T) {
			// act
			subtest.input1.updateTime(&subtest.input2)

			// assert
			if subtest.input1 != subtest.expected {
				test.Errorf("test %q failed for %v != %v", subtest.name, subtest.input1, subtest.expected)
			}
		})
	}
}
