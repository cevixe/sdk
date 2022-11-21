package factory

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
)

type AwsFactory interface {
	DynamodbClient(region ...string) dynamodbiface.DynamoDBAPI
	SnsClient(region ...string) snsiface.SNSAPI
	S3Client(region ...string) s3iface.S3API
}

const (
	DynamoDB = "dynamodb"
	SNS      = "sns"
	S3       = "s3"
)
