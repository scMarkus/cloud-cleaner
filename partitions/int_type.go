package partitions

import (
	"strconv"

	"smartclip.de/cloud-cleaner/types"
)

type intValue struct {
	int
}

func (self intValue) ToString() string {
	return strconv.Itoa(self.int)
}

func (self intValue) Smaller(other types.TypedPartitionValue) bool {
	return self.int < other.(intValue).int
}
