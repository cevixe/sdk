package dynamodb

import (
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// Use at your own risk!

// Most of the code below is adapted from:
// https://github.com/aws/aws-sdk-go-v2/blob/feature/dynamodb/attributevalue/v1.0.6/feature/dynamodb/attributevalue/convert.go

// If instead you want "github.com/aws/aws-sdk-go-v2/service/dynamodb/types":
// just change the import and only take FromDynamoDBEventAVMap, FromDynamoDBEventAVList and FromDynamoDBEventAV
// OR just use attributevalue.FromDynamoDBStreamsMap() (which the below code was adapted from):
// attributevalue.FromDynamoDBStreamsMap(record.Dynamodb.Keys)
// attributevalue.FromDynamoDBStreamsMap(record.Dynamodb.NewImage)
// attributevalue.FromDynamoDBStreamsMap(record.Dynamodb.OldImage)

// FromDynamoDBEvent converts a Lambda Event DynamoDBEvent,
// including all nested members, to a slice of dynamodbstreams Record
func FromDynamoDBEvent(from events.DynamoDBEvent) ([]types.Record, error) {
	return FromDynamoDBEventRecords(from.Records)
}

// FromDynamoDBEventRecords converts a slice of Lambda Event DynamoDBEventRecord,
// including all nested members, to a slice of dynamodbstreams Record
func FromDynamoDBEventRecords(from []events.DynamoDBEventRecord) ([]types.Record, error) {
	records := make([]types.Record, len(from))
	for i, v := range from {
		record, err := FromDynamoDBEventRecord(v)
		if err != nil {
			return nil, err
		}
		records[i] = record
	}
	return records, nil
}

// FromDynamoDBEvent converts a Lambda Event DynamoDBEventRecord,
// including all nested members, to a dynamodbstreams Record
func FromDynamoDBEventRecord(from events.DynamoDBEventRecord) (types.Record, error) {
	streamViewType := types.StreamViewType(from.Change.StreamViewType)
	var principal *string
	var userIdentType *string
	if from.UserIdentity != nil {
		principal = &from.UserIdentity.PrincipalID
		userIdentType = &from.UserIdentity.Type
	}

	keys, err := FromDynamoDBEventAVMap(from.Change.Keys)
	if err != nil {
		return types.Record{}, err
	}
	var newImage map[string]types.AttributeValue
	if streamViewType == types.StreamViewTypeNewImage || streamViewType == types.StreamViewTypeNewAndOldImages {
		newImage, err = FromDynamoDBEventAVMap(from.Change.NewImage)
		if err != nil {
			return types.Record{}, err
		}
	}
	var oldImage map[string]types.AttributeValue
	if streamViewType == types.StreamViewTypeOldImage || streamViewType == types.StreamViewTypeNewAndOldImages {
		oldImage, err = FromDynamoDBEventAVMap(from.Change.OldImage)
		if err != nil {
			return types.Record{}, err
		}
	}
	return types.Record{
		AwsRegion: &from.AWSRegion,
		Dynamodb: &types.StreamRecord{
			ApproximateCreationDateTime: &from.Change.ApproximateCreationDateTime.Time,
			Keys:                        keys,
			NewImage:                    newImage,
			OldImage:                    oldImage,
			SequenceNumber:              &from.Change.SequenceNumber,
			SizeBytes:                   &from.Change.SizeBytes,
			StreamViewType:              streamViewType,
		},
		EventID:      &from.EventID,
		EventName:    types.OperationType(from.EventName),
		EventSource:  &from.EventSource,
		EventVersion: &from.EventVersion,
		UserIdentity: &types.Identity{
			PrincipalId: principal,
			Type:        userIdentType,
		},
	}, nil
}

// FromDynamoDBEventMap converts a map of Lambda Event DynamoDB
// AttributeValues, including all nested members, to a dynamodbstreams map of AttributeValue.
func FromDynamoDBEventAVMap(from map[string]events.DynamoDBAttributeValue) (to map[string]types.AttributeValue, err error) {
	to = make(map[string]types.AttributeValue, len(from))
	for field, value := range from {
		to[field], err = FromDynamoDBEventAV(value)
		if err != nil {
			return nil, err
		}
	}

	return to, nil
}

// FromDynamoDBEventList converts a slice of Lambda Event DynamoDB
// AttributeValues, including all nested members, to a slice ofdynamodbstreams AttributeValue.
func FromDynamoDBEventAVList(from []events.DynamoDBAttributeValue) (to []types.AttributeValue, err error) {
	to = make([]types.AttributeValue, len(from))
	for i := 0; i < len(from); i++ {
		to[i], err = FromDynamoDBEventAV(from[i])
		if err != nil {
			return nil, err
		}
	}

	return to, nil
}

// FromDynamoDBEvent converts a Lambda Event DynamoDB AttributeValue, including
// all nested members, to a dynamodbstreams AttributeValue.
func FromDynamoDBEventAV(from events.DynamoDBAttributeValue) (types.AttributeValue, error) {
	switch from.DataType() {
	case events.DataTypeNull:
		return &types.AttributeValueMemberNULL{Value: from.IsNull()}, nil

	case events.DataTypeBoolean:
		return &types.AttributeValueMemberBOOL{Value: from.Boolean()}, nil

	case events.DataTypeBinary:
		return &types.AttributeValueMemberB{Value: from.Binary()}, nil

	case events.DataTypeBinarySet:
		bs := make([][]byte, len(from.BinarySet()))
		for i := 0; i < len(from.BinarySet()); i++ {
			bs[i] = append([]byte{}, from.BinarySet()[i]...)
		}
		return &types.AttributeValueMemberBS{Value: bs}, nil

	case events.DataTypeNumber:
		return &types.AttributeValueMemberN{Value: from.Number()}, nil

	case events.DataTypeNumberSet:
		return &types.AttributeValueMemberNS{Value: append([]string{}, from.NumberSet()...)}, nil

	case events.DataTypeString:
		return &types.AttributeValueMemberS{Value: from.String()}, nil

	case events.DataTypeStringSet:
		return &types.AttributeValueMemberSS{Value: append([]string{}, from.StringSet()...)}, nil

	case events.DataTypeList:
		values, err := FromDynamoDBEventAVList(from.List())
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberL{Value: values}, nil

	case events.DataTypeMap:
		values, err := FromDynamoDBEventAVMap(from.Map())
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberM{Value: values}, nil

	default:
		return nil, fmt.Errorf("unknown AttributeValue union member, %T", from)
	}
}
