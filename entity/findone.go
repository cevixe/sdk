package entity

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/jsii-runtime-go"
	cvxcontext "github.com/cevixe/sdk/context"
	"github.com/pkg/errors"
)

type FindOneProps struct {
	Domain   string `field:"required"`
	Typename string `field:"required"`
	ID       string `field:"required"`
}

func FindOne(ctx context.Context, props *FindOneProps) (Entity, error) {

	cvxini := cvxcontext.GetInitContenxt(ctx)
	app := cvxini.AppName
	table := fmt.Sprintf("dyn-%s-%s-statestore", app, props.Domain)

	input := &dynamodb.GetItemInput{
		TableName: jsii.String(table),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: props.ID},
		},
	}

	output, err := cvxini.DynamodbClient.GetItem(ctx, input)

	if err != nil {
		return nil, errors.Wrap(err, "cannot get dynamodb entity by id")
	}

	if output.Item == nil {
		return nil, nil
	}

	entity, err := FromDynamodb_TableMap(output.Item)
	if err != nil {
		return nil, errors.Wrap(err, "cannot read dynamodb entity map")
	}

	if entity.Type() != props.Typename {
		return nil, errors.New("invalid entity typename")
	}

	return entity, nil
}
