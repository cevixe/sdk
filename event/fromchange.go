package event

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodbstreams/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/cevixe/sdk/common/dynamodb"
	"github.com/pkg/errors"
	"github.com/stoewer/go-strcase"
)

type Map = map[string]interface{}

func From_DynamoDBEventRecord(record events.DynamoDBEventRecord) (Event, error) {

	entityMap, err := getDynamoDBEntityMap(record)
	if err != nil {
		return nil, errors.Wrap(err, "invalid dynamodb stream record")
	}

	event, err := mapEventFromEntityMap(entityMap)
	if err != nil {
		return nil, errors.Wrap(err, "cannot map event from entity map")
	}

	return event, nil
}

func getDynamoDBEntityMap(record events.DynamoDBEventRecord) (Map, error) {

	if record.EventName == "REMOVE" {
		return nil, errors.New("physical record deletion not allowed")
	}

	dynRecord, err := dynamodb.FromDynamoDBEventRecord(record)
	if err != nil {
		return nil, errors.Wrap(err, "invalid dynamodb event record")
	}

	entityMap := make(Map)
	image := dynRecord.Dynamodb.NewImage
	if err = attributevalue.UnmarshalMap(image, &entityMap); err != nil {
		return nil, errors.Wrap(err, "invalid dynamodb record")
	}

	if err = validateEntityMapRequiredFields(entityMap); err != nil {
		return nil, errors.Wrap(err, "invalid entity map")
	}

	setEntityMapDefaultValues(dynRecord, entityMap)

	return entityMap, nil
}

func validateEntityMapRequiredFields(entityMap Map) error {
	requiredFields := []string{
		"__typename",
		"__space",
		"__status",
		"__transaction",
		"id",
		"version",
		"updatedBy",
		"updatedAt",
		"createdBy",
		"createdAt",
	}
	for _, field := range requiredFields {
		if entityMap[field] == nil {
			message := fmt.Sprintf("required field `%s` not found", field)
			return errors.New(message)
		}
	}
	return nil
}

func setEntityMapDefaultValues(dynRecord types.Record, entityMap Map) {

	if entityMap["__eventtype"] == nil {
		if entityMap["__status"] == nil {
			entityMap["__eventtype"] = "deleted"
		} else if dynRecord.EventName == "INSERT" {
			entityMap["__eventtype"] = "created"
		} else {
			entityMap["__eventtype"] = "updated"
		}
	}

	if entityMap["__eventversion"] == nil {
		entityMap["__eventversion"] = 1.0
	}

	if entityMap["__eventdata"] == nil {
		privateFields := []string{
			"__typename",
			"__space",
			"__status",
			"__transaction",
			"__eventtype",
			"__eventversion",
			"__eventdata",
		}
		data := make(map[string]interface{})
		for field, value := range entityMap {
			data[field] = value
		}
		for _, field := range privateFields {
			delete(data, field)
		}
		entityMap["__eventdata"] = data
	}
}

func mapEventFromEntityMap(entityMap Map) (Event, error) {

	event := &impl{}

	entityId := entityMap["id"].(string)
	entityTypename := entityMap["__typename"].(string)
	entityVersion := entityMap["version"].(float64)
	entityUpdatedAt := entityMap["updatedAt"].(string)
	entityUpdatedBy := entityMap["updatedBy"].(string)
	entityTransaction := entityMap["__transaction"].(string)

	eventType := entityMap["__eventtype"].(string)
	eventVersion := entityMap["__eventversion"].(float64)
	eventData := entityMap["__eventdata"]

	event.EventSource = fmt.Sprintf("/domain/%s/%s",
		strcase.KebabCase(entityTypename), entityId)

	event.EventID = fmt.Sprintf("%20d", int(entityVersion))

	event.EventType = fmt.Sprintf("%s.%s.v%d",
		strcase.KebabCase(entityTypename),
		strcase.KebabCase(eventType),
		int(eventVersion),
	)

	event.EventTime = entityUpdatedAt
	event.EventContentType = "application/json"
	event.EventUser = entityUpdatedBy
	event.EventTransaction = entityTransaction

	eventDataString, err := json.Marshal(eventData)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal event data")
	}
	event.EventData = string(eventDataString)

	return event, nil
}
