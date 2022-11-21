package entity

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/jsii-runtime-go"
	cvxcontext "github.com/cevixe/sdk/context"
	"github.com/pkg/errors"
	"github.com/stoewer/go-strcase"
)

type FindOneProps struct {
	Typename string `field:"required"`
	ID       string `field:"required"`
}

func FindOne(ctx context.Context, props *FindOneProps) (Entity, error) {

	cvx := cvxcontext.GetCevixeContext(ctx)
	app := cvx.ApplicationName
	domain := strcase.KebabCase(props.Typename)
	table := fmt.Sprintf("dyn-%s-%s-statestore", app, domain)

	output, err := cvx.DynamodbClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: jsii.String(table),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: props.ID},
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot get dynamodb entity by id")
	}

	entity, err := FromDynamodb_TableMap(output.Item)
	if err != nil {
		return nil, errors.Wrap(err, "cannot read dynamodb entity map")
	}
	return entity, nil
}
