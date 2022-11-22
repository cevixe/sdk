package result

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/jsii-runtime-go"
	cvxcontext "github.com/cevixe/sdk/context"
	"github.com/cevixe/sdk/entity"
	"github.com/cevixe/sdk/message"
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
		} else if item.Status() == entity.EntityStatus_Dead {
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
	itemBuffer, _ := json.Marshal(item)
	log.Println(string(itemBuffer))

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
		if propsToAvoid[key] {
			continue
		}
		if value == nil {
			name := fmt.Sprintf("#%s", key)
			expressionAttributeNames[name] = key
			expressionRemove = append(expressionRemove, name)
		} else {
			switch value.(type) {
			case *types.AttributeValueMemberNULL:
				fieldName := fmt.Sprintf("#%s", key)
				expressionRemove = append(expressionRemove, fieldName)
				expressionAttributeNames[fieldName] = key
			default:
				fieldName := fmt.Sprintf("#%s", key)
				fieldValue := fmt.Sprintf(":%s", key)
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

	conditionExpression := "#__status = :__status AND #version = :version"

	expressionAttributeNames["#__status"] = "__status"
	expressionAttributeValues[":__status"] = &types.AttributeValueMemberS{Value: string(entity.EntityStatus_Alive)}

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

	jsonBuffer, _ := json.Marshal(update)
	log.Println(string(jsonBuffer))

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
		pk := fmt.Sprintf("__%s-pk", idx)
		expressionRemove = append(expressionRemove, fmt.Sprintf("#%s", pk))
		expressionAttributeNames[fmt.Sprintf("#%s", pk)] = pk
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
		fieldName := fmt.Sprintf("#%s", field)
		fieldValue := fmt.Sprintf(":%s", field)
		expressionSet[fieldName] = fieldValue
		expressionAttributeNames[fieldName] = field
		expressionAttributeValues[fieldValue] = item[field]
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

	conditionExpression := "#__status = :__status AND #version = :version"

	expressionAttributeNames["#__status"] = "__status"
	expressionAttributeValues[":__status"] = &types.AttributeValueMemberS{Value: string(entity.EntityStatus_Alive)}

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

	jsonBuffer, _ := json.Marshal(update)
	log.Println(string(jsonBuffer))

	return &types.TransactWriteItem{Update: update}, nil
}
