package execution

import (
	"os"
	"testing"

	"smartclip.de/cloud-cleaner/config"
)

func TestGetArchivingConfig(test *testing.T) {
	// arrange
	testTabel := []struct {
		name     string
		input    string
		expected string
		err      bool
	}{
		{
			name: "no file error",
			err:  true,
		},
		{
			name:  "some broken jsonnet",
			input: `{"Resources":[}`,
			err:   true,
		},
		{
			name: "parse success",
			input: `{
        "Resources":[
          {
            "Name": "resource",
            "PartitionProvider": {
              Name: "s3_hive"
            },
            PartitionSpec: {
              Name: 'day',
              DataType: 'date',
            },
             "Prefix": "s3://prefix"
          },
        ]
      }`,
			expected: "resource",
		},
	}

	for _, subtest := range testTabel {
		test.Run(subtest.name, func(t *testing.T) {
			// arrange
			var conf config.RawConfig
			if len(subtest.input) > 0 {
				content := []byte(subtest.input)
				os.WriteFile("./archiving_config.jsonnet", content, 0644)

				defer os.Remove("./archiving_config.jsonnet")
			}

			// act
			err := conf.Setup()

			// assert
			if (err == nil) == subtest.err {
				test.Errorf("%q failed with unexpected error %q", subtest.name, err)
			}

			if len(subtest.expected) > 0 {
				// only check without errors to make sure at least one resource entry is in array
				if conf.Resources[0].Name != subtest.expected {
					test.Errorf("test %q failed for %v != %v", subtest.name, conf.Resources, subtest.expected)
				}
			}
		})
	}
}
