package context

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

const (
	CevixeInitContextKey string = "cvxini"
)

type InitContext struct {
	AppName        string
	DomainName     string
	HandlerName    string
	S3Client       *s3.Client
	SNSClient      *sns.Client
	DynamodbClient *dynamodb.Client
}

func GetInitContenxt(ctx context.Context) *InitContext {
	return ctx.Value(CevixeInitContextKey).(*InitContext)
}
