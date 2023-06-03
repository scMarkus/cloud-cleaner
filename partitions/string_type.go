package partitions

import "smartclip.de/cloud-cleaner/types"

type stringValue struct {
	string
}

func (self stringValue) ToString() string {
	return self.string
}

func (self stringValue) Smaller(other types.TypedPartitionValue) bool {
	return self.string < other.(stringValue).string
}
