package result

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/jsii-runtime-go"
	cvxcontext "github.com/cevixe/sdk/context"
	"github.com/cevixe/sdk/entity"
	"github.com/cevixe/sdk/message"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
)

func Write(ctx context.Context, result Result) error {
	cvxini := cvxcontext.GetInitContenxt(ctx)
	statestore := fmt.Sprintf("dyn-%s-%s-statestore", cvxini.AppName, cvxini.DomainName)
	commandstore := fmt.Sprintf("dyn-%s-core-commandstore", cvxini.AppName)
	input, err := generateTransactWriteItemsInput(statestore, commandstore, result)
	if err != nil {
		return errors.Wrap(err, "cannot generate dynamodb transaction input")
	}
	if _, err = cvxini.DynamodbClient.TransactWriteItems(ctx, input); err != nil {
		return errors.Wrap(err, "cannot execute dynamodb transaction")
	}
	return nil
}

func generateTransactWriteItemsInput(statestore string, commandstore string, result Result) (*dynamodb.TransactWriteItemsInput, error) {
	items := make([]types.TransactWriteItem, 0)

	for _, item := range result.GetEntities() {
		if item.Version() == 1 {
			insert, err := generateTransactEntityInsert(statestore, item)
			if err != nil {
				return nil, errors.Wrap(err, "cannot generate transact entity insert")
			}
			items = append(items, *insert)
		} else if item.Status() == entity.EntityStatus_Alive {
			update, err := generateTransactEntityUpdate(statestore, item)
			if err != nil {
				return nil, errors.Wrap(err, "cannot generate transact entity update")
			}
			items = append(items, *update)
		} else {
			delete, err := generateTransactEntityDelete(statestore, item)
			if err != nil {
				return nil, errors.Wrap(err, "cannot generate transact entity delete")
			}
			items = append(items, *delete)
		}
	}

	for _, item := range result.GetCommands() {
		insert, err := generateTransactMessageInsert(commandstore, item)
		if err != nil {
			return nil, errors.Wrap(err, "cannot generate transact command insert")
		}
		items = append(items, *insert)
	}

	return &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	}, nil
}

func generateTransactMessageInsert(table string, input message.Message) (*types.TransactWriteItem, error) {
	item, err := message.ToDynamodb_Map(input)
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

func generateTransactEntityInsert(table string, input entity.Entity) (*types.TransactWriteItem, error) {
	item, err := entity.ToDynamodb_Map(input)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal entity to dynamodb map")
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

func generateTransactEntityUpdate(table string, input entity.Entity) (*types.TransactWriteItem, error) {
	item, err := entity.ToDynamodb_Map(input)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal entity to dynamodb map")
	}

	updateExpression := ""
	expressionAttributeNames := make(map[string]string)
	expressionAttributeValues := make(map[string]types.AttributeValue)

	expressionSet := make(map[string]string)
	expressionRemove := make([]string, 0)

	propsToAvoid := map[string]bool{
		"__typename": true,
		"id":         true,
		"version":    true,
		"__status":   true,
		"__space":    true,
		"createdAt":  true,
		"createdBy":  true,
	}

	for key, value := range item {
		alias := ulid.Make().String()
		fieldName := fmt.Sprintf("#%s", alias)
		fieldValue := fmt.Sprintf(":%s", alias)
		if propsToAvoid[key] {
			continue
		}
		if value == nil {
			expressionAttributeNames[fieldName] = key
			expressionRemove = append(expressionRemove, fieldName)
		} else {
			switch value.(type) {
			case *types.AttributeValueMemberNULL:
				expressionAttributeNames[fieldName] = key
				expressionRemove = append(expressionRemove, fieldName)
			default:
				expressionSet[fieldName] = fieldValue
				expressionAttributeNames[fieldName] = key
				expressionAttributeValues[fieldValue] = value
			}
		}
	}

	if len(expressionSet) > 0 {
		updateExpression = "SET"
		for key, value := range expressionSet {
			updateExpression = fmt.Sprintf("%s %s = %s,", updateExpression, key, value)
		}
		updateExpression = updateExpression[:len(updateExpression)-1]
	}

	if len(expressionRemove) > 0 {
		updateExpression = fmt.Sprintf("%s %s", updateExpression, "REMOVE")
		for _, key := range expressionRemove {
			updateExpression = fmt.Sprintf("%s %s,", updateExpression, key)
		}
		updateExpression = updateExpression[:len(updateExpression)-1]
	}

	conditionExpression := "#status = :status AND #version = :version"

	expressionAttributeNames["#status"] = "__status"
	expressionAttributeValues[":status"] = &types.AttributeValueMemberS{Value: string(entity.EntityStatus_Alive)}

	previousVersion := strconv.FormatUint(input.Version()-1, 10)
	expressionAttributeNames["#version"] = "version"
	expressionAttributeValues[":version"] = &types.AttributeValueMemberN{Value: previousVersion}

	update := &types.Update{
		TableName:                 jsii.String(table),
		Key:                       map[string]types.AttributeValue{"id": item["id"]},
		UpdateExpression:          jsii.String(updateExpression),
		ConditionExpression:       jsii.String(conditionExpression),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	}

	return &types.TransactWriteItem{Update: update}, nil
}

func generateTransactEntityDelete(table string, input entity.Entity) (*types.TransactWriteItem, error) {
	item, err := entity.ToDynamodb_Map(input)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal entity to dynamodb map")
	}

	updateExpression := ""
	expressionAttributeNames := make(map[string]string)
	expressionAttributeValues := make(map[string]types.AttributeValue)

	expressionSet := make(map[string]string)
	expressionRemove := make([]string, 0)

	for _, idx := range input.Indexes() {
		alias := ulid.Make().String()
		pk := fmt.Sprintf("__%s-pk", idx)
		fieldName := fmt.Sprintf("#%s", alias)
		expressionRemove = append(expressionRemove, fieldName)
		expressionAttributeNames[fieldName] = pk
	}

	fieldsToUpdate := []string{
		"updatedAt",
		"updatedBy",
		"__status",
		"__space",
		"__transaction",
		"__eventtype",
		"__eventversion",
		"__eventtrigger",
		"__eventdata",
	}

	for _, field := range fieldsToUpdate {
		alias := ulid.Make().String()
		fieldName := fmt.Sprintf("#%s", alias)
		fieldValue := fmt.Sprintf(":%s", alias)
		value := item[field]
		if value == nil {
			expressionAttributeNames[fieldName] = field
			expressionRemove = append(expressionRemove, fieldName)
		} else {
			switch value.(type) {
			case *types.AttributeValueMemberNULL:
				expressionAttributeNames[fieldName] = field
				expressionRemove = append(expressionRemove, fieldName)
			default:
				expressionSet[fieldName] = fieldValue
				expressionAttributeNames[fieldName] = field
				expressionAttributeValues[fieldValue] = value
			}
		}
	}

	if len(expressionSet) > 0 {
		updateExpression = "SET"
		for key, value := range expressionSet {
			updateExpression = fmt.Sprintf("%s %s = %s,", updateExpression, key, value)
		}
		updateExpression = updateExpression[:len(updateExpression)-1]
	}

	if len(expressionRemove) > 0 {
		updateExpression = fmt.Sprintf("%s %s", updateExpression, "REMOVE")
		for _, key := range expressionRemove {
			updateExpression = fmt.Sprintf("%s %s,", updateExpression, key)
		}
		updateExpression = updateExpression[:len(updateExpression)-1]
	}

	conditionExpression := "#status = :expectedStatus AND #version = :expectedVersion"

	expressionAttributeNames["#status"] = "__status"
	expressionAttributeValues[":expectedStatus"] = &types.AttributeValueMemberS{Value: string(entity.EntityStatus_Alive)}

	previousVersion := strconv.FormatUint(input.Version()-1, 10)
	expressionAttributeNames["#version"] = "version"
	expressionAttributeValues[":expectedVersion"] = &types.AttributeValueMemberN{Value: previousVersion}

	update := &types.Update{
		TableName:                 jsii.String(table),
		Key:                       map[string]types.AttributeValue{"id": item["id"]},
		UpdateExpression:          jsii.String(updateExpression),
		ConditionExpression:       jsii.String(conditionExpression),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	}

	return &types.TransactWriteItem{Update: update}, nil
}
