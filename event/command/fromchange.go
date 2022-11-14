package command

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodbstreams/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/jsii-runtime-go"
	"github.com/cevixe/sdk/common/dynamodb"
	"github.com/cevixe/sdk/common/iso8601"
	"github.com/cevixe/sdk/common/json"
	"github.com/google/uuid"
	"github.com/stoewer/go-strcase"
)

type commandRecord struct {
	Domain      *string                 `json:"domain"`
	Type        *string                 `json:"type"`
	ID          *string                 `json:"id"`
	Version     *uint64                 `json:"version"`
	Time        *time.Time              `json:"time"`
	User        *string                 `json:"user"`
	Transaction *string                 `json:"transaction"`
	Data        *map[string]interface{} `json:"data"`
}

func From_DynamoDBEventRecord(record events.DynamoDBEventRecord) Command {

	commandRecord := getDynamoDBCommandRecord(record)
	if commandRecord == nil {
		log.Fatal("invalid dynamodb stream record")
	}

	return mapCommandFromDynamoDBCommandRecord(commandRecord)
}

func getDynamoDBCommandRecord(record events.DynamoDBEventRecord) *commandRecord {

	if record.EventName == "INSERT" {
		return nil
	}

	dynRecord, err := dynamodb.FromDynamoDBEventRecord(record)
	if err != nil {
		return nil
	}

	image := dynRecord.Dynamodb.NewImage
	commandRecord := &commandRecord{}
	err = attributevalue.UnmarshalMap(image, commandRecord)
	if err != nil {
		return nil
	}

	if !validDynamoDBCommandRecordMandatoryFields(commandRecord) {
		return nil
	}

	setDynamoDBCommandRecordDefaultValues(dynRecord, commandRecord)

	return commandRecord
}

func validDynamoDBCommandRecordMandatoryFields(commandRecord *commandRecord) bool {
	if commandRecord.Domain == nil ||
		commandRecord.Type == nil ||
		commandRecord.ID == nil ||
		commandRecord.Time == nil ||
		commandRecord.User == nil ||
		commandRecord.Data == nil {
		return false
	}
	return true
}

func setDynamoDBCommandRecordDefaultValues(dynRecord types.Record, commandRecord *commandRecord) {

	if commandRecord.Transaction == nil {
		commandRecord.Transaction = jsii.String(uuid.NewString())
	}

	if commandRecord.Version == nil {
		var defaultVersion uint64 = 1
		commandRecord.Version = &defaultVersion
	}
}

func mapCommandFromDynamoDBCommandRecord(commandRecord *commandRecord) Command {

	command := &impl{}

	command.CommandSource = fmt.Sprintf("/command/%s/%s",
		strcase.KebabCase(*commandRecord.Domain), *commandRecord.Type)

	command.CommandID = *commandRecord.ID

	command.CommandType = fmt.Sprintf("%s.%s.v%d",
		strcase.KebabCase(*commandRecord.Domain),
		strcase.KebabCase(*commandRecord.Type),
		*commandRecord.Version,
	)

	command.CommandTime = iso8601.FromTime(*commandRecord.Time)

	command.CommandContentType = "application/json"

	command.CommandUser = *commandRecord.User

	command.CommandTransaction = *commandRecord.Transaction

	command.CommandData = json.Marshal(*commandRecord.Data)

	return command
}
