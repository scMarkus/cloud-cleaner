package main

import (
	"log"

	"smartclip.de/cloud-cleaner/config"
	"smartclip.de/cloud-cleaner/execution"
)

type test struct {
	nested []nested
}

type nested struct {
	val []string
}

func main() {
	log.Printf("runtime config setup:")
	conf, err := config.Setup()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("partition collection:")
	if err := execution.StartProviders(&conf); err != nil {
		log.Fatal(err)
	}

	log.Printf("execution lock:")
	if err := execution.CreateExecutionLocks(&conf); err != nil {
		log.Fatal(err)
	}

	log.Printf("filter partitions:")
	if err := execution.FilterKeptPartitions(&conf); err != nil {
		log.Fatal(err)
	}

	log.Printf("check targets:")
	if err := execution.CheckOperationTargets(&conf); err != nil {
		log.Fatal(err)
	}

	log.Printf("execute action:")
	if err := execution.ExecuteArmedAction(&conf); err != nil {
		log.Fatal(err)
	}

	log.Println("finished")
	//for hash, partition := range conf.Resources["adpod_hourly"].GetPartitions() {
	//	log.Printf("name: %q - locks %#v", hash, partition.GetDependencies())
	//}

	//for _, resource := range conf.Resources {
	//	//log.Printf("resource: %q", resource.Name)
	//	for _, partition := range resource.Partitions {
	//		//log.Printf("partition: %q", k)
	//		if len(partition.Dependencies) > 0 {
	//			var tmp string
	//			partition.HashCompatibleEncoding(&tmp)
	//			log.Printf("%s depends on %s", tmp, partition.Dependencies[0].Resource.Name)
	//		}
	//	}
	//}
}
