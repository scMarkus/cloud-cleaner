package providers

import (
	"fmt"
	"log"
	"strings"

	"smartclip.de/cloud-cleaner/types"
)

func (provider TrinoClient) CopyPartition(
	partititons types.PartitionList,
	source types.RuntimeResource,
	target types.RuntimeResource,
) (types.PreparedActions, error) {

	return nil, nil
}

func (provider TrinoClient) RemovePartition(partititons types.PartitionList, source types.RuntimeResource) (types.PreparedActions, error) {
	var preparedActions types.PreparedActions

	resource, _ := source.(*trinoRuntimeResource) // was validated at resource creation

	columns := make([]string, len(resource.PartitionSpec))
	for idx, column := range resource.PartitionSpec {
		columns[idx] = fmt.Sprintf(`'%s'`, column.Name)
	}

	for _, partition := range partititons {
		rawPartitionVals := partition.GetValues()
		partitionVals := make([]string, len(rawPartitionVals))
		for idx, val := range rawPartitionVals {
			columns[idx] = fmt.Sprintf(`'%s'`, val)
		}

		sql := fmt.Sprintf(
			"system.unregister_partition(%s, %s, ARRAY[%s], ARRAY[%s])",
			resource.schema,
			resource.table,
			strings.Join(columns, ","),
			strings.Join(partitionVals, ","),
		)

		log.Printf("prepare %s", sql)

		action := func() error {
			log.Printf("execute: %s", sql)
			if _, err := provider.db.Query(sql); err != nil {
				return err
			}

			return nil
		}

		preparedActions = append(preparedActions, types.PreparedPartitionAction{
			Partition: partition,
			Action:    action,
		})
	}

	return preparedActions, nil
}
