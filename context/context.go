package context

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
)

const (
	CevixeContextKey string = "cvx"
)

type CevixeContext struct {
	Actor            string
	Transaction      string
	Trigger          string
	ApplicationName  string
	StateStoreName   string
	EventStoreName   string
	CommandStoreName string
	DynamodbClient   dynamodb.Client
}

func GetCevixeContext(ctx context.Context) *CevixeContext {
	defaultContext := &CevixeContext{
		Actor:       "unknown",
		Trigger:     "",
		Transaction: uuid.NewString(),
	}
	value := ctx.Value(CevixeContextKey)
	if value == nil {
		return defaultContext
	}
	return value.(*CevixeContext)
}
