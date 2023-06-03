package execution

import (
	"log"
	"sync"

	"smartclip.de/cloud-cleaner/config"
	"smartclip.de/cloud-cleaner/types"
)

func FilterKeptPartitions(conf *config.RuntimeConfig) error {
	var wg sync.WaitGroup
	errChan := make(chan error)

	for _, operation := range conf.Operations {
		wg.Add(1)
		go func(operation types.RuntimeOperationSingle) {
			defer wg.Done()

			log.Printf("exclude partitions for operation %q", operation.GetOperationName())

			partitions := operation.GetOperationSource().GetPartitions()
			log.Printf("partition count pre filter for operation %q: %d", operation.GetOperationName(), len(partitions))

			partitionList, err := operation.GetKeptPartitions()
			if err != nil {
				errChan <- err
			}

			log.Printf("partition count after filter for operation %q: %d", operation.GetOperationName(), len(partitionList))
		}(operation)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	err := <-errChan
	if err != nil {
		return err
	}

	return nil
}
