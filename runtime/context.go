package runtime

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/cevixe/sdk/client/config"
	cvxcontext "github.com/cevixe/sdk/context"
)

func NewContext() context.Context {

	ctx := context.Background()
	cfg := config.NewConfig(ctx)

	appName := os.Getenv("CVX_APP_NAME")
	domainName := os.Getenv("CVX_DOMAIN_NAME")
	handlerName := os.Getenv("CVX_HANDLER_NAME")
	s3Client := s3.NewFromConfig(cfg)
	snsClient := sns.NewFromConfig(cfg)
	dynamodbClient := dynamodb.NewFromConfig(cfg)

	ctx = context.WithValue(ctx, cvxcontext.CevixeInitContextKey,
		&cvxcontext.InitContext{
			AppName:        appName,
			DomainName:     domainName,
			HandlerName:    handlerName,
			S3Client:       s3Client,
			SNSClient:      snsClient,
			DynamodbClient: dynamodbClient,
		})
	return ctx
}
