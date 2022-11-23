package message

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/jsii-runtime-go"
	cvxcontext "github.com/cevixe/sdk/context"
	"github.com/pkg/errors"
)

func Write(ctx context.Context, msg ...Message) error {
	cvxini := cvxcontext.GetInitContenxt(ctx)
	input, err := generateTransactWriteItemsInput(cvxini.AppName, msg...)
	if err != nil {
		return errors.Wrap(err, "cannot generate dynamodb transaction input")
	}
	if _, err = cvxini.DynamodbClient.TransactWriteItems(ctx, input); err != nil {
		return errors.Wrap(err, "cannot execute dynamodb transaction")
	}
	return nil
}

func generateTransactWriteItemsInput(app string, msg ...Message) (*dynamodb.TransactWriteItemsInput, error) {
	items := make([]types.TransactWriteItem, 0)

	for _, item := range msg {
		insert, err := generateTransactMessageInsert(app, item)
		if err != nil {
			return nil, errors.Wrap(err, "cannot generate transact command insert")
		}
		items = append(items, *insert)
	}

	return &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	}, nil
}

func generateTransactMessageInsert(app string, input Message) (*types.TransactWriteItem, error) {
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
	table := fmt.Sprintf("dyn-%s-core-%sstore", app, input.Kind())
	return &types.TransactWriteItem{
		Put: &types.Put{
			TableName: jsii.String(table),
			Item:      item,
		},
	}, nil
}
