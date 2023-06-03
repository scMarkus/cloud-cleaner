package config

import (
	"fmt"

	"github.com/spf13/viper"

	ops "smartclip.de/cloud-cleaner/operations"
	"smartclip.de/cloud-cleaner/types"
)

func getOperations(rawConfig *viper.Viper, resources map[string]types.RuntimeResource) (map[string]types.RuntimeOperationSingle, map[string]types.RuntimeResource, error) {
	if ok := rawConfig.IsSet("operations"); !ok {
		return nil, nil, fmt.Errorf("no operation definition found")
	}
	operationsList, ok := rawConfig.Get("operations").([]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("operation config is not an array")
	}

	operations := make(map[string]types.RuntimeOperationSingle, len(operationsList))
	sources := make(map[string]types.RuntimeResource, len(operationsList))

	for _, rawOperation := range operationsList {
		operationSpec, ok := rawOperation.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("some operation is not of type map")
		}

		// make sure action exists
		val, ok := operationSpec["action"]
		if !ok {
			return nil, nil, fmt.Errorf("some operation has no \"action\" field")
		}

		str, ok := val.(string)
		if !ok || str == "" {
			return nil, nil, fmt.Errorf("\"action\" field of some operation is not of type string")
		}

		action := types.Action(str)
		makeOperationFunc, ok := ops.KnownActions[action]
		if !ok {
			return nil, nil, fmt.Errorf("some operation has invalid action %q", action)
		}

		operation, err := makeOperationFunc(operationSpec, resources)
		if err != nil {
			return nil, nil, err
		}

		s := operation.GetOperationSource()
		sources[s.GetResourceName()] = s // duplicate sources are fine here

		// ensure name uniqness
		if _, ok := operations[operation.GetOperationName()]; ok {
			return nil, nil, fmt.Errorf("duplicate operation name %q", operation.GetOperationName())
		}

		operations[operation.GetOperationName()] = operation
	}

	// if target of one operation is source of another we get race conditions
	// more complex dependency traversal may be viable in the future
	for operationName, operation := range operations {
		opeartionDouble, ok := operation.(types.RuntimeOperationDouble)
		if !ok {
			continue
		}

		if _, ok := sources[opeartionDouble.GetOperationTarget().GetResourceName()]; ok {
			return nil, nil, fmt.Errorf("source %q is target es well", operationName)
		}
	}

	return operations, sources, nil
}
