package execution

import (
	"fmt"
	"sync"

	"smartclip.de/cloud-cleaner/config"
	"smartclip.de/cloud-cleaner/types"
)

func CheckOperationTargets(conf *config.RuntimeConfig) error {
	var wg sync.WaitGroup
	errChan := make(chan error)

	for _, operation := range conf.Operations {
		wg.Add(1)
		go func(operation types.RuntimeOperation) {
			defer wg.Done()

			if op, ok := operation.(types.RuntimeOperationDouble); ok {
				resource := op.GetOperationTarget()
				targetPartitions := resource.GetPartitions()

				sourcePartitions, err := op.GetKeptPartitions()
				if err != nil {
					errChan <- err
				}

				for _, sourcePartition := range sourcePartitions {
					if _, ok := targetPartitions[sourcePartition.GetParsedValues().ToString()]; ok {
						errChan <- fmt.Errorf("operation %q has source partition in target already", op.GetOperationName())
					}
				}
			}
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
