package message

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodbstreams/attributevalue"
	"github.com/cevixe/sdk/common/dynamodb"
	"github.com/pkg/errors"
)

func FromStream(input events.DynamoDBEventRecord) (Message, error) {

	messageMap, err := getDynamoDBMessageMap(input)
	if err != nil {
		return nil, errors.Wrap(err, "invalid dynamodb stream record")
	}

	messageJson, err := json.Marshal(messageMap)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal stream message map")
	}

	return FromJson(messageJson)
}

func getDynamoDBMessageMap(record events.DynamoDBEventRecord) (map[string]interface{}, error) {

	if record.EventName == "REMOVE" {
		return nil, errors.New("physical record deletion not allowed")
	}

	dynRecord, err := dynamodb.FromDynamoDBEventRecord(record)
	if err != nil {
		return nil, errors.Wrap(err, "invalid dynamodb event record")
	}

	messageMap := make(map[string]interface{})
	image := dynRecord.Dynamodb.NewImage
	if err = attributevalue.UnmarshalMap(image, &messageMap); err != nil {
		return nil, errors.Wrap(err, "invalid dynamodb record")
	}

	if err = validateMessageMapRequiredFields(messageMap); err != nil {
		return nil, errors.Wrap(err, "invalid entity map")
	}

	return messageMap, nil
}

func validateMessageMapRequiredFields(messageMap map[string]interface{}) error {
	requiredFields := []string{
		"source",
		"id",
		"kind",
		"type",
		"time",
		"contentType",
		"encodingType",
		"data",
		"author",
		"trigger",
		"transaction",
	}
	for _, field := range requiredFields {
		if messageMap[field] == nil {
			message := fmt.Sprintf("required field `%s` not found", field)
			return errors.New(message)
		}
	}
	return nil
}
