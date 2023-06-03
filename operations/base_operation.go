package operations

import (
	"fmt"

	"smartclip.de/cloud-cleaner/exclude"
	"smartclip.de/cloud-cleaner/types"
)

type BaseOperation struct {
	Name      string
	Excludes  []types.Exclude
	DependsOn []string
}

func (operation BaseOperation) GetOperationName() string {
	return operation.Name
}

func (operation BaseOperation) GetDependencies() []string {
	return operation.DependsOn
}

// dont check dependencies hiere since all runtime operations musst be parsed first
func makeBaseOperation(conf map[string]interface{}, resources map[string]types.RuntimeResource) (BaseOperation, error) {
	var (
		val          interface{}
		ok           bool
		name         string
		excludes     []types.Exclude
		dependencies []string
	)

	if val, ok = conf["name"]; !ok {
		return BaseOperation{}, fmt.Errorf("some operation has no name field")
	}
	if name, ok = val.(string); !ok || name == "" {
		return BaseOperation{}, fmt.Errorf("\"name\" field of some operation is not of type string")
	}

	if val, ok = conf["dependson"]; ok {
		rawDependencies, ok := val.([]interface{})
		if !ok {
			return BaseOperation{}, fmt.Errorf("operation %q has non array dependencies", name)
		}

		dependencies = make([]string, len(rawDependencies))
		for idx, rawDependency := range rawDependencies {
			dependency, ok := rawDependency.(string)
			if !ok {
				return BaseOperation{}, fmt.Errorf("operation %q has non string denpendency", name)
			}
			dependencies[idx] = dependency
		}
	}

	if val, ok = conf["exclude"]; ok {
		rawExcludes, ok := val.([]interface{})
		if !ok {
			return BaseOperation{}, fmt.Errorf("operation %q has non array excludes", name)
		}

		excludes = make([]types.Exclude, len(rawExcludes))
		for idx, rawExclude := range rawExcludes {
			excludeConf, ok := rawExclude.(map[string]interface{})
			if !ok {
				return BaseOperation{}, fmt.Errorf("exclude  number %q of operation %q is not of a map", idx, name)
			}

			exclude, err := exclude.MakeExclude(name, excludeConf)
			if err != nil {
				return BaseOperation{}, err
			}

			excludes[idx] = exclude
		}
	}
	return BaseOperation{name, excludes, dependencies}, nil
}
