package types

/*
this is likeyly the most important function of this app
it has multiple responsebilities

1. provide the function executing one action for exactly one partition
2. it must wait for all execution locks to complete first
3. communicating (as a log message) what is happening
4. clean up the partitinos execution lock by goling Partition.CloseCompleteChan()

additionally it must not error when executed concurrently
there may be provider which are sensitive to that
*/
type PreparedActions []PreparedPartitionAction

type PreparedPartitionAction struct {
	Partition
	Action func() error
}

// this is just a dummy - since it has no source there is nothing to operate on
type RuntimeOperation interface {
	GetOperationName() string
	GetDependencies() []string
	PartitionsWithExcludes() error
}

type RuntimeOperationSingle interface {
	RuntimeOperation
	GetOperationSource() RuntimeResource
	GetKeptPartitions() (PartitionList, error)

	ExecuteOperation() (PreparedActions, error)
}

type RuntimeOperationDouble interface {
	RuntimeOperationSingle
	GetOperationTarget() RuntimeResource
}
