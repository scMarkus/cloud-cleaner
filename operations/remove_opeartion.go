package operations

import (
	"fmt"

	"smartclip.de/cloud-cleaner/types"
)

type RemoveOperation struct {
	OperationSingle
}

func makeRemoveOpeartion(conf map[string]interface{}, resources map[string]types.RuntimeResource) (types.RuntimeOperationSingle, error) {
	var (
		val          interface{}
		ok           bool
		resourceName string
		resource     types.RuntimeResource
	)

	baseOperation, err := makeBaseOperation(conf, resources)
	if err != nil {
		return nil, err
	}

	moveOperation := RemoveOperation{OperationSingle: OperationSingle{BaseOperation: baseOperation}}

	if val, ok = conf["source"]; !ok {
		return nil, fmt.Errorf("operation %q has no source configured", moveOperation.Name)
	}
	if resourceName, ok = val.(string); !ok || resourceName == "" {
		return nil, fmt.Errorf("\"source\" field of operation %q is not of type string", moveOperation.Name)
	}
	if resource, ok = resources[resourceName]; !ok {
		return nil, fmt.Errorf("configured source %q of opaartion %q is no known resource", moveOperation.Name, resourceName)
	}
	moveOperation.source = resource

	return &moveOperation, nil
}

func (operation RemoveOperation) ExecuteOperation() (types.PreparedActions, error) {
	provider, ok := operation.source.GetProvider().(types.RemoveProvider)
	if !ok {
		return nil, fmt.Errorf("provider of resource %q does not implement remove operation", operation.source.GetResourceName())
	}

	preparedAction, err := provider.RemovePartition(operation.keptPartitions, operation.source)
	if err != nil {
		return nil, err
	}

	return preparedAction, nil
}
