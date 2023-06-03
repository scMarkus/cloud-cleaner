package config

import (
	"log"
	"os"
	"strings"

	"github.com/google/go-jsonnet"
)

func getRawConfig(rawConfigBytes *[]byte) (err error) {
	configFilePath, ok := os.LookupEnv("ARCHIVING_CONFIG")
	if !ok {
		configFilePath = "./archiving_config.jsonnet"
	}

	*rawConfigBytes, err = os.ReadFile(configFilePath)
	if err != nil {
		return
	}

	if strings.HasSuffix(configFilePath, "jsonnet") {
		vm := jsonnet.MakeVM()
		jsonnetStr, err := vm.EvaluateAnonymousSnippet(configFilePath, string(*rawConfigBytes))
		if err != nil {
			return err
		}

		// print evaluated jasonet for debugging purpose
		log.Printf("following json config was generated from jsonnet: %s", jsonnetStr)
		*rawConfigBytes = []byte(jsonnetStr)
	}

	return
}
