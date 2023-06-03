package operations

import "smartclip.de/cloud-cleaner/types"

const (
	UnknowAction types.Action = ""
	Replicate                 = "copy"   // copy is go internal name
	Remove                    = "delete" // delete is go internal name
)

type makeOperation map[types.Action]func(map[string]interface{}, map[string]types.RuntimeResource) (types.RuntimeOperationSingle, error)

var KnownActions makeOperation = makeOperation{
	Replicate: makeReplicateOpeartion,
	Remove:    makeRemoveOpeartion,
}
