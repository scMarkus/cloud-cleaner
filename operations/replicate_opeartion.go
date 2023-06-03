package operations

import (
	"fmt"

	"smartclip.de/cloud-cleaner/types"
)

// this operation by accident has the same attributes as the move operation (for now)
type ReplicateOperation struct {
	OperationSingle
	target types.RuntimeResource
}

func makeReplicateOpeartion(conf map[string]interface{}, resources map[string]types.RuntimeResource) (types.RuntimeOperationSingle, error) {
	var (
		val          interface{}
		ok           bool
		resourceName string
	)

	baseOperation, err := makeBaseOperation(conf, resources)
	if err != nil {
		return nil, err
	}

	removeOperation := ReplicateOperation{OperationSingle: OperationSingle{BaseOperation: baseOperation}}

	if val, ok = conf["source"]; !ok {
		return nil, fmt.Errorf("operation %q has no source configured", removeOperation.Name)
	}
	if resourceName, ok = val.(string); !ok || resourceName == "" {
		return nil, fmt.Errorf("\"source\" field of operation %q is not of type string", removeOperation.Name)
	}
	if removeOperation.source, ok = resources[resourceName]; !ok {
		return nil, fmt.Errorf("configured source %q of operation %q is no known resource", removeOperation.Name, resourceName)
	}

	if val, ok = conf["target"]; !ok {
		return nil, fmt.Errorf("operation %q has no target configured", removeOperation.Name)
	}
	if resourceName, ok = val.(string); !ok || resourceName == "" {
		return nil, fmt.Errorf("\"target\" field of operation %q is not of type string", removeOperation.Name)
	}
	if removeOperation.target, ok = resources[resourceName]; !ok {
		return nil, fmt.Errorf("configured target %q of operation %q is no known resource", removeOperation.Name, resourceName)
	}

	if removeOperation.source.GetProvider().GetProviderName() != removeOperation.target.GetProvider().GetProviderName() {
		return nil, fmt.Errorf("operation envolving source and target must have same provider (%q)", removeOperation.Name)
	}

	return &removeOperation, nil
}

func (operation ReplicateOperation) ExecuteOperation() (types.PreparedActions, error) {
	provider, ok := operation.source.GetProvider().(types.ReplicateProvider)
	if !ok {
		return nil, fmt.Errorf("provider of resource %q does not implement copy operation", operation.source.GetResourceName())
	}

	preparedAction, err := provider.CopyPartition(operation.keptPartitions, operation.source, operation.target)
	if err != nil {
		return nil, err
	}

	return preparedAction, nil
}
