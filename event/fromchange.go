package event

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

type entityRecord struct {
	Type         *string                 `json:"__typename"`
	ID           *string                 `json:"id"`
	Version      *uint64                 `json:"version"`
	CreatedAt    *time.Time              `json:"createdAt"`
	CreatedBy    *string                 `json:"createdBy"`
	UpdatedAt    *time.Time              `json:"updatedAt"`
	UpdatedBy    *string                 `json:"updatedBy"`
	Archived     *bool                   `json:"__archived"`
	Transaction  *string                 `json:"__transaction"`
	EventType    *string                 `json:"__eventtype"`
	EventVersion *uint64                 `json:"__eventversion"`
	EventData    *map[string]interface{} `json:"__eventdata"`
}

func From_DynamoDBEventRecord(record events.DynamoDBEventRecord) Event {

	entityRecord := getDynamoDBEntityRecord(record)
	if entityRecord == nil {
		log.Fatal("invalid dynamodb stream record")
	}

	return mapEventFromDynamoDBEntityRecord(entityRecord)
}

func getDynamoDBEntityRecord(record events.DynamoDBEventRecord) *entityRecord {

	jsonString := json.Marshal(record)
	log.Printf("EventRecord: %s\n", jsonString)

	if record.EventName == "REMOVE" {
		log.Printf("EventName failed: %s\n", record.EventName)
		return nil
	}

	dynRecord, err := dynamodb.FromDynamoDBEventRecord(record)
	if err != nil {
		log.Printf("dynRecord failed: %v\n", err)
		return nil
	}
	log.Printf("dynRecord ok: %v\n", json.Marshal(dynRecord))

	image := dynRecord.Dynamodb.NewImage
	log.Printf("newImage ok: %v\n", json.Marshal(image))
	entityRecord := &entityRecord{}
	genericMap := make(map[string]interface{})
	attributevalue.UnmarshalMap(image, &genericMap)
	genericMapString := json.Marshal(genericMap)
	log.Printf("generic map: %s\n", genericMapString)
	err = attributevalue.UnmarshalMap(image, entityRecord)
	if err != nil {
		log.Printf("entityRecord failed: %v\n", err)
		return nil
	}

	if !validDynamoDBEntityRecordMandatoryFields(entityRecord) {
		entityString := json.Marshal(entityRecord)
		log.Printf("validation failed: %s\n", entityString)
		return nil
	}

	setDynamoDBEntityRecordDefaultValues(dynRecord, entityRecord)

	return entityRecord
}

func validDynamoDBEntityRecordMandatoryFields(entityRecord *entityRecord) bool {
	if entityRecord.Type == nil ||
		entityRecord.ID == nil ||
		entityRecord.Version == nil ||
		entityRecord.UpdatedBy == nil ||
		entityRecord.UpdatedAt == nil ||
		entityRecord.CreatedBy == nil ||
		entityRecord.CreatedAt == nil ||
		entityRecord.Archived == nil {
		return false
	}
	return true
}

func setDynamoDBEntityRecordDefaultValues(dynRecord types.Record, entityRecord *entityRecord) {

	if entityRecord.Transaction == nil {
		entityRecord.Transaction = jsii.String(uuid.NewString())
	}

	if entityRecord.EventType == nil {
		if *entityRecord.Archived {
			entityRecord.EventType = jsii.String("deleted")
		} else if dynRecord.EventName == "INSERT" {
			entityRecord.EventType = jsii.String("created")
		} else {
			entityRecord.EventType = jsii.String("updated")
		}
	}

	if entityRecord.EventVersion == nil {
		var defaultVersion uint64 = 1
		entityRecord.EventVersion = &defaultVersion
	}

	if entityRecord.EventData == nil {
		reservedFields := []string{
			"__typename", "__section", "__archived", "__transaction",
			"__eventtype", "__eventversion", "__eventdata",
		}
		data := make(map[string]interface{})
		err := attributevalue.UnmarshalMap(dynRecord.Dynamodb.NewImage, data)
		if err != nil {
			log.Fatalf("cannot unmarshall dynamodb event record: %v", err)
		}
		for _, item := range reservedFields {
			delete(data, item)
		}
	}
}

func mapEventFromDynamoDBEntityRecord(entityRecord *entityRecord) Event {

	event := &impl{}

	event.EventSource = fmt.Sprintf("/domain/%s/%s",
		strcase.KebabCase(*entityRecord.Type), *entityRecord.ID)

	event.EventID = fmt.Sprintf("%20d", *entityRecord.Version)

	event.EventType = fmt.Sprintf("%s.%s.v%d",
		strcase.KebabCase(*entityRecord.Type),
		strcase.KebabCase(*entityRecord.EventType),
		*entityRecord.EventVersion,
	)

	event.EventTime = iso8601.FromTime(*entityRecord.UpdatedAt)

	event.EventContentType = "application/json"

	event.EventUser = *entityRecord.UpdatedBy

	event.EventTransaction = *entityRecord.Transaction

	event.EventData = json.Marshal(*entityRecord.EventData)

	return event
}
