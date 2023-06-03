package partitions

import (
	"time"

	"smartclip.de/cloud-cleaner/types"
)

type timeValue struct {
	time.Time
}

func (self timeValue) ToString() string {
	return self.Time.String()
}

func (self timeValue) Smaller(other types.TypedPartitionValue) bool {
	return self.Time.Before(other.(timeValue).Time)
}
