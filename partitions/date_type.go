package partitions

import (
	"time"

	"smartclip.de/cloud-cleaner/types"
)

type dateValue struct {
	time.Time
}

func (self dateValue) ToString() string {
	return self.Time.String()
}

func (self dateValue) Smaller(other types.TypedPartitionValue) bool {
	return self.Time.Before(other.(dateValue).Time)
}
