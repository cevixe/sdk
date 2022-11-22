package entity

import (
	"encoding/json"
	"fmt"
	"strings"

	tablevalue "github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	streamvalue "github.com/aws/aws-sdk-go-v2/feature/dynamodbstreams/attributevalue"
	tabletypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/pkg/errors"
)

func FromDynamodb_TableMap(input map[string]tabletypes.AttributeValue) (Entity, error) {

	imageMap := make(map[string]interface{})

	if err := tablevalue.UnmarshalMap(input, &imageMap); err != nil {
		return nil, errors.Wrap(err, "invalid dynamodb record")
	}

	if err := validateEntityMapRequiredFields(imageMap); err != nil {
		return nil, errors.Wrap(err, "invalid entity map")
	}

	entityMap := imageMapToEntityMap(imageMap)

	entityJson, err := json.Marshal(entityMap)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal stream entity map")
	}

	return FromJson(entityJson)
}

func FromDynamodb_StreamMap(input map[string]streamtypes.AttributeValue) (Entity, error) {

	imageMap := make(map[string]interface{})

	if err := streamvalue.UnmarshalMap(input, &imageMap); err != nil {
		return nil, errors.Wrap(err, "invalid dynamodb record")
	}

	if err := validateEntityMapRequiredFields(imageMap); err != nil {
		return nil, errors.Wrap(err, "invalid entity map")
	}

	entityMap := imageMapToEntityMap(imageMap)

	entityJson, err := json.Marshal(entityMap)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal stream entity map")
	}

	return FromJson(entityJson)
}

func validateEntityMapRequiredFields(entityMap map[string]interface{}) error {
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

func imageMapToEntityMap(imageMap map[string]interface{}) map[string]interface{} {

	entityMap := make(map[string]interface{})

	entityMap["type"] = imageMap["__typename"]
	entityMap["id"] = imageMap["id"]
	entityMap["version"] = imageMap["version"]
	entityMap["status"] = imageMap["__status"]
	entityMap["updatedAt"] = imageMap["updatedAt"]
	entityMap["updatedBy"] = imageMap["updatedBy"]
	entityMap["createdAt"] = imageMap["createdAt"]
	entityMap["createdBy"] = imageMap["createdBy"]
	entityMap["lastTransaction"] = imageMap["__transaction"]
	entityMap["lastEventTrigger"] = imageMap["__eventtrigger"]
	entityMap["lastEventType"] = imageMap["__eventtype"]
	entityMap["lastEventVersion"] = imageMap["__eventversion"]
	entityMap["lastEventData"] = imageMap["__eventdata"]

	indexes := make([]string, 0)
	for key := range imageMap {
		if strings.HasPrefix(key, "__") &&
			strings.HasSuffix(key, "-pk") {
			indexes = append(indexes, key[2:len(key)-3])
		}
	}
	entityMap["indexes"] = indexes

	metadataFields := []string{
		"__typename",
		"id",
		"version",
		"__status",
		"__space",
		"updatedAt",
		"updatedBy",
		"createdAt",
		"createdBy",
		"__transaction",
		"__eventtrigger",
		"__eventtype",
		"__eventversion",
		"__eventdata",
	}

	for _, field := range metadataFields {
		delete(imageMap, field)
	}

	entityMap["data"] = imageMap

	return entityMap
}
