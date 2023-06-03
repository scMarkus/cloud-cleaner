package execution

import (
	"fmt"
	"log"
	"sync"

	"smartclip.de/cloud-cleaner/config"
)

func CreateExecutionLocks(conf *config.RuntimeConfig) error {
	var wg sync.WaitGroup

	for _, operation := range conf.Operations {
		log.Printf("setup execution locks for operation %q", operation.GetOperationName())
		currentResource := operation.GetOperationSource()

		// register every operation in this partition so it waits longer until sending completation
		for _, partition := range currentResource.GetPartitions() {
			partition.WaitForCompletion()
		}

		for _, dependency := range operation.GetDependencies() {
			otherOperation, ok := conf.Operations[dependency]
			if !ok {
				return fmt.Errorf(
					"oppeartion %q assumed dependency %q which does not exist",
					operation.GetOperationName(),
					dependency,
				)
			}
			otherResource := otherOperation.GetOperationSource()

			wg.Add(1)
			go func() {
				defer wg.Done()

				for partitionHash, currentPartition := range currentResource.GetPartitions() {
					if otherPartition, ok := otherResource.GetPartitions()[partitionHash]; ok {
						log.Printf(
							"blocking partition %q for %q by %q",
							partitionHash,
							currentResource.GetResourceName(),
							otherResource.GetResourceName(),
						)

						wgOther := otherPartition.RegisterCompletionLock()
						log.Printf("wait sync: %+v - %p", currentPartition, wgOther)
						currentPartition.AddDependencies(wgOther)
					}
				}
			}()
		}
	}

	wg.Wait()

	return nil
}
