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

type FindByProps struct {
	Domain     string `field:"required"`
	Typename   string `field:"required"`
	IndexName  string `field:"required"`
	IndexValue string `field:"required"`
	NextToken  string `field:"optional"`
	Limit      uint64 `field:"optional"`
}

func FindBy(ctx context.Context, props *FindByProps) (EntityPage, error) {

	cvxini := cvxcontext.GetInitContenxt(ctx)
	app := cvxini.AppName
	table := fmt.Sprintf("dyn-%s-%s-statestore", app, props.Domain)

	partitionKey := fmt.Sprintf("__%s-pk", props.IndexName)
	input := &dynamodb.QueryInput{
		TableName:              jsii.String(table),
		IndexName:              jsii.String(props.IndexName),
		KeyConditionExpression: jsii.String("#index = :value"),
		FilterExpression:       jsii.String("#type = :type"),
		ExpressionAttributeNames: map[string]string{
			"#index": partitionKey,
			"#type":  "__typename",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":value": &types.AttributeValueMemberS{
				Value: props.IndexValue,
			},
			":type": &types.AttributeValueMemberS{
				Value: props.Typename,
			},
		},
		ScanIndexForward: jsii.Bool(false),
	}

	if props.Limit == 0 {
		var defaultLimit int32 = 20
		input.Limit = &defaultLimit
	} else {
		var customLimit int32 = int32(props.Limit)
		input.Limit = &customLimit
	}

	if props.NextToken != "" {
		input.ExclusiveStartKey = map[string]types.AttributeValue{
			partitionKey: &types.AttributeValueMemberS{
				Value: props.IndexValue,
			},
			"id": &types.AttributeValueMemberS{
				Value: props.NextToken,
			},
		}
	}

	output, err := cvxini.DynamodbClient.Query(ctx, input)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get dynamodb entity by id")
	}

	entities := make([]Entity, 0)
	for _, item := range output.Items {
		entity, err := FromDynamodb_TableMap(item)
		if err != nil {
			return nil, errors.Wrap(err, "cannot read dynamodb entity map")
		}
		entities = append(entities, entity)
	}
	nextToken := ""
	if len(output.LastEvaluatedKey) > 0 {
		attribute := output.LastEvaluatedKey["id"].(*types.AttributeValueMemberS)
		nextToken = attribute.Value
	}

	return NewPage(entities, nextToken), nil
}
