package providers

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"database/sql"
	"github.com/trinodb/trino-go-client/trino"

	"smartclip.de/cloud-cleaner/partitions"
	"smartclip.de/cloud-cleaner/resources"
	"smartclip.de/cloud-cleaner/types"
)

type TrinoPartition struct {
	partitions.BasePartition
	ts time.Time
}

type trinoRuntimeResource struct {
	resources.BaseResource
	catalog string
	schema  string
	table   string
}

func (partition *TrinoPartition) GetTimestamp() (time.Time, error) {
	return time.Time{}, fmt.Errorf("trino partitions do not support timestamp related actions")
}

func (currentPartition *TrinoPartition) UpdatePartition(updatePartition types.Partition) error {
	otherPartition, ok := updatePartition.(*TrinoPartition)
	if !ok {
		return fmt.Errorf("partition has incorrect type for update")
	}

	if currentPartition.ts.After(otherPartition.ts) {
		currentPartition.ts = otherPartition.ts
	}

	return nil
}

type TrinoClient struct {
	BaseProvider
	db *sql.DB
}

func (provider *TrinoClient) Init(providerConf map[string]interface{}, errChan chan<- error, wg *sync.WaitGroup) {
	var host, catalog, schema string
	val, ok := providerConf["config"]
	if !ok {
		errChan <- fmt.Errorf("config fild of trino provider does not exist")
	}
	conf, ok := val.(map[string]interface{})
	if !ok {
		errChan <- fmt.Errorf("config of trino provider os not of map type")
	}

	// mandatory parameter
	parameter := "hosturi"
	tmp, ok := conf[parameter]
	if !ok {
		errChan <- fmt.Errorf("provider conf parameter %q not found", parameter)
	}
	if host = tmp.(string); host == "" {
		errChan <- fmt.Errorf("provider conf parameter %q has no value", parameter)
	}

	// optional parameter
	parameter = "catalog"
	if tmp, ok = conf[parameter]; ok {
		if catalog, ok = tmp.(string); catalog == "" || !ok {
			errChan <- fmt.Errorf("provider conf parameter %q has no value", parameter)
		}
	}

	// optional parameter
	parameter = "schema"
	if tmp, ok = conf[parameter]; ok {
		if schema, ok = tmp.(string); schema == "" || !ok {
			errChan <- fmt.Errorf("provider conf parameter %q has no value", parameter)
		}
	}

	dsnConf := trino.Config{
		ServerURI: host,
		Catalog:   catalog,
		Schema:    schema,
	}
	dsn, err := dsnConf.FormatDSN()
	if err != nil {
		errChan <- err
	}
	db, err := sql.Open("trino", dsn)
	if err != nil {
		errChan <- err
	}

	provider.db = db
	provider.BaseProvider, err = MakeBaseProvider(providerConf)
	if err != nil {
		errChan <- err
	}

	wg.Done()
}

func (provider *TrinoClient) GetRelatedResources() []types.RuntimeResource {
	return provider.Resources
}

func (provider *TrinoClient) MakeRuntimResource(conf map[string]interface{}) (types.RuntimeResource, error) {
	var (
		tmp interface{}
		ok  bool
		err error
	)

	baseresource, err := resources.MakeBaseRuntimResource(conf)
	baseresource.Provider = provider
	resource := trinoRuntimeResource{BaseResource: baseresource}

	if err != nil {
		return nil, err
	}

	if tmp, ok = conf["table"]; !ok {
		return nil, fmt.Errorf("resource %q has no table field", resource.Name)
	}
	fqtn, ok := tmp.(string)
	if !ok {
		return nil, fmt.Errorf("table field of resource %q is not of type string", resource.Name)
	}

	parts := strings.SplitN(fqtn, ".", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("trino table definition of resource %q must follow schema \"<catalog>.<schema>.<table>\"", resource.Name)
	}
	resource.catalog = parts[0]
	resource.schema = parts[1]
	resource.table = parts[2]

	provider.Resources = append(provider.Resources, &resource)
	return &resource, nil
}

func (client TrinoClient) ResourceInputChan() chan<- types.RuntimeResource {
	return client.InputChan
}

func (provider TrinoClient) CheckAccess(errChan chan<- error, wg *sync.WaitGroup) {
	_, err := provider.db.Query("show tables")
	if err != nil {
		errChan <- err
	}

	wg.Done()
}

// concurrent and mutable since every goroutine uniqually handles one resource
func (client TrinoClient) CollectPartitions(errorChan chan<- error, wg *sync.WaitGroup) {
	for r := range client.InputChan {
		resource := r.(*trinoRuntimeResource)
		log.Printf("trino collection start for %q", resource.Name)

		sqlQuery := partionQueryString(resource)
		log.Printf("trino query: %s", sqlQuery)
		rows, err := client.db.Query(sqlQuery)
		if err != nil {
			errorChan <- err
			return
		}

		for rows.Next() {
			// generic interface because of trino row() type slice helps with arbitrary amount
			// of partition columns since trino sql clients validates &row against being a pointer
			// and scan() attribute having as many elements as columns specified in the sql query
			var trinoPartitionStrings trino.NullSliceString
			if err := rows.Scan(&trinoPartitionStrings); err != nil {
				errorChan <- err
				return
			}

			partitionValues := make([]string, len(resource.PartitionSpec))
			for idx, partitionString := range trinoPartitionStrings.SliceString {
				partitionValues[idx] = partitionString.String
			}

			partition := TrinoPartition{
				BasePartition: partitions.BasePartition{
					PartitionValues: partitionValues,
					Resource:        resource,
					CompletionWg:    &sync.WaitGroup{},
				},
			}

			parsedValues, err := partitions.ParsePartitionString(resource.PartitionSpec, partition.PartitionValues)
			if err != nil {
				errorChan <- err
				return
			}
			partition.TypedPartitionValues = parsedValues

			// no checks for partition collision since if so trino / HMS would be broken already
			hashId := partition.TypedPartitionValues.ToString()
			resource.Partitions[hashId] = &partition
		}
		rows.Close()

		// Check for errors from iterating over rows.
		if err := rows.Err(); err != nil {
			errorChan <- err
			return
		}

		log.Printf("%s found %d partitions for table: %s", resource.Name, len(resource.Partitions), resource.table)
		wg.Done()
	}
}

func partionQueryString(resource *trinoRuntimeResource) string {
	columns := make([]string, len(resource.PartitionSpec))
	for idx, column := range resource.PartitionSpec {
		columns[idx] = fmt.Sprintf(`CAST(%s AS VARCHAR)`, column.Name)
	}

	sql := fmt.Sprintf(
		`SELECT ARRAY[%s] FROM %s.%s."%s$partitions"`,
		strings.Join(columns, ", "),
		resource.catalog,
		resource.schema,
		resource.table,
	)

	return sql
}
