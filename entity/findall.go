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

type FindAllProps struct {
	Typename  string `field:"required"`
	NextToken string `field:"optional"`
	Limit     uint64 `field:"optional"`
}

func FindAll(ctx context.Context, props *FindAllProps) (EntityPage, error) {

	cvxini := cvxcontext.GetInitContenxt(ctx)
	app := cvxini.AppName
	domain := strcase.KebabCase(props.Typename)
	table := fmt.Sprintf("dyn-%s-%s-statestore", app, domain)

	input := &dynamodb.QueryInput{
		TableName:              jsii.String(table),
		IndexName:              jsii.String("by-space"),
		KeyConditionExpression: jsii.String("#space = :space"),
		ExpressionAttributeNames: map[string]string{
			"#space": "__space",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":space": &types.AttributeValueMemberS{
				Value: fmt.Sprintf("%s#%s", EntityStatus_Alive, domain),
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
			"__space": &types.AttributeValueMemberS{
				Value: fmt.Sprintf("%s#%s", EntityStatus_Alive, domain),
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
