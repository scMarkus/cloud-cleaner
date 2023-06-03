package types

import (
	"strings"
	"sync"
	"time"
)

type PartitionDependencies []*sync.WaitGroup

// have this type implement sort interface
type PartitionList []Partition

func (list PartitionList) Len() int {
	return len(list)
}

func (list PartitionList) Less(i, j int) bool {
	otherParsedValues := list[j].GetParsedValues()
	for idx, p1 := range list[i].GetParsedValues() {
		if p1.Smaller(otherParsedValues[idx]) {
			return true
		}
	}
	return false
}

func (list PartitionList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

type PartitionSpec struct {
	Name     string
	DataType DataType
}

// this enables dummy structs with a konkrete type to hold a parsed partition value in a Partition
type TypedPartitionValue interface {
	Smaller(TypedPartitionValue) bool
	ToString() string
}

type TypedPartitionValueList []TypedPartitionValue

func (vals TypedPartitionValueList) ToString() string {
	tmpList := make([]string, len(vals))
	for idx, val := range vals {
		tmpList[idx] = val.ToString()
	}

	return strings.Join(tmpList, "\t")
}

type Partition interface {
	GetValues() []string
	GetDependencies() PartitionDependencies
	GetParsedValues() TypedPartitionValueList
	GetTimestamp() (time.Time, error)

	AddDependencies(*sync.WaitGroup)
	UpdatePartition(Partition) error

	RegisterCompletionLock() *sync.WaitGroup
	WaitForCompletion()
	CloseCompleteChan()
}
