package providers

import "smartclip.de/cloud-cleaner/types"

const (
	UnknownProviderType types.ProviderType = ""
	S3HiveProviderType                     = "s3Hive"
	S3KeyProviderType                      = "s3Key"
	TrinoProviderType                      = "trino"
)

var KnownProviderTypes map[types.ProviderType]interface{} = map[types.ProviderType]interface{}{
	S3HiveProviderType: nil,
	S3KeyProviderType:  nil,
	TrinoProviderType:  nil,
}
