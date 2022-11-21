package message

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/jsii-runtime-go"
	cvxcontext "github.com/cevixe/sdk/context"
	"github.com/pkg/errors"
)

func Write(ctx context.Context, msg ...Message) error {
	cvx := cvxcontext.GetCevixeContext(ctx)
	input, err := generateTransactWriteItemsInput(cvx, msg...)
	if err != nil {
		return errors.Wrap(err, "cannot generate dynamodb transaction input")
	}
	if _, err = cvx.DynamodbClient.TransactWriteItems(ctx, input); err != nil {
		return errors.Wrap(err, "cannot execute dynamodb transaction")
	}
	return nil
}

func generateTransactWriteItemsInput(cvx *cvxcontext.CevixeContext, msg ...Message) (*dynamodb.TransactWriteItemsInput, error) {
	items := make([]types.TransactWriteItem, 0)

	for _, item := range msg {
		insert, err := generateTransactMessageInsert(cvx.CommandStoreName, item)
		if err != nil {
			return nil, errors.Wrap(err, "cannot generate transact command insert")
		}
		items = append(items, *insert)
	}

	return &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	}, nil
}

func generateTransactMessageInsert(table string, input Message) (*types.TransactWriteItem, error) {
	item, err := ToDynamodb_Map(input)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal message to dynamodb map")
	}
	for key, value := range item {
		if value == nil {
			delete(item, key)
		} else {
			switch value.(type) {
			case *types.AttributeValueMemberNULL:
				delete(item, key)
			default:
				continue
			}
		}
	}

	return &types.TransactWriteItem{
		Put: &types.Put{
			TableName:           jsii.String(table),
			Item:                item,
			ConditionExpression: jsii.String("attribute_not_exists(#id)"),
			ExpressionAttributeNames: map[string]string{
				"#id": "id",
			},
		},
	}, nil
}
