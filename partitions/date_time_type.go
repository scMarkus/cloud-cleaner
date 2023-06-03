package partitions

import (
	"time"

	"smartclip.de/cloud-cleaner/types"
)

type dateTimeValue struct {
	time.Time
}

func (self dateTimeValue) ToString() string {
	return self.Time.String()
}

func (self dateTimeValue) Smaller(other types.TypedPartitionValue) bool {
	return self.Time.Before(other.(dateTimeValue).Time)
}
