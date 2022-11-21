package entity

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/cevixe/sdk/common/dynamodb"
	"github.com/pkg/errors"
)

func FromStream(input events.DynamoDBEventRecord) (Entity, error) {

	if input.EventName == "REMOVE" {
		return nil, errors.New("physical record deletion not allowed")
	}

	dynRecord, err := dynamodb.FromDynamoDBEventRecord(input)
	if err != nil {
		return nil, errors.Wrap(err, "invalid dynamodb event record")
	}

	entity, err := FromDynamodb_StreamMap(dynRecord.Dynamodb.NewImage)
	if err != nil {
		return nil, errors.Wrap(err, "cannot unmarshal dynamodb stream map to entity")
	}

	return entity, nil
}
