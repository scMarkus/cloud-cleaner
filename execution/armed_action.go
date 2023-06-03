package execution

import (
	"log"
	"sync"
	"time"

	"smartclip.de/cloud-cleaner/config"
	"smartclip.de/cloud-cleaner/types"
)

func ExecuteArmedAction(conf *config.RuntimeConfig) error {
	var wgOperation sync.WaitGroup

	timeoutSeconds := time.Second                    // TODO: make configurable
	globaleCompletionChan := make(chan struct{}, 10) // buffer to reduce completion wait
	timeoutChan := make(chan time.Time)

	globaleCompletionChan <- struct{}{} // init timeout in async function

	errChan := make(chan error)
	if !conf.Armed {
		log.Println("executing as dry run")
	}

	go func() {
		for {
			select {
			// reset timeout in case of any partition completing
			case <-globaleCompletionChan:
				// convert send only channel to common channel by extracting value (<-()) and setting (()<-)
				timeoutChan <- (<-time.After(timeoutSeconds))
			case <-timeoutChan:
				// this is needed since time.After() returns read only chan which can not be closed
				close(timeoutChan)
				return
			}
		}
	}()

	for _, operation := range conf.Operations {
		wgOperation.Add(1)
		go func(operation types.RuntimeOperationSingle) {
			defer wgOperation.Done()
			log.Printf("prepare operation: %q", operation.GetOperationName())

			preparedActions, err := operation.ExecuteOperation()
			if err != nil {
				errChan <- err
				return
			}

			if conf.Armed {
				// TODO: it might make sense to parallelize this as well
				for _, preparedPartition := range preparedActions {
					partition := preparedPartition.Partition

					// wait for dependencies to finish first
					waitChan := make(chan struct{})
					go func() {
						for _, wgDependency := range partition.GetDependencies() {
							// will unblock when dependency partitions get finished
							wgDependency.Wait()
						}
						close(waitChan)
					}()

					// either unblock by cleared dependencies or error on timeout
					select {
					case <-waitChan:
						// TODO: this in dangerous
						//if err := preparedPartition.Action(); err != nil {
						//	errChan <- err
						//	return
						//}

						partition.CloseCompleteChan()
						globaleCompletionChan <- struct{}{}
					case <-timeoutChan:
						log.Printf(
							"partition %q of operation %q timed out (could not unblock)",
							partition.GetParsedValues().ToString(),
							operation.GetOperationName(),
						)
					}

				}
			}
		}(operation)
	}

	go func() {
		wgOperation.Wait()
		close(errChan)
	}()

	err := <-errChan
	if err != nil {
		return err
	}

	return nil
}
