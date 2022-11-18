package event

import (
	"fmt"
	"time"

	"github.com/cevixe/sdk/common/json"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/jsii-runtime-go"
)

func To_SNSPublishBatchRequestEntry(event Event) types.PublishBatchRequestEntry {

	data := make(map[string]interface{})
	event.Data(&data)
	message := json.Marshal(data)
	entryId := fmt.Sprintf("%s/%s", event.Source(), event.ID())

	return types.PublishBatchRequestEntry{
		Id:                jsii.String(entryId),
		Subject:           jsii.String(event.Type()),
		MessageGroupId:    jsii.String(event.Source()),
		Message:           jsii.String(message),
		MessageAttributes: generateMessageAttributes(event),
	}
}

func To_SNSPublishInput(event Event) sns.PublishInput {

	data := make(map[string]interface{})
	event.Data(&data)
	message := json.Marshal(data)

	fmt.Println(event.Source())
	fmt.Println(event.Type())
	fmt.Println(event.ID())
	return sns.PublishInput{
		Subject:           jsii.String(event.Type()),
		MessageGroupId:    jsii.String(event.Source()),
		Message:           jsii.String(message),
		MessageAttributes: generateMessageAttributes(event),
	}
}

func generateMessageAttributes(event Event) map[string]types.MessageAttributeValue {

	return map[string]types.MessageAttributeValue{
		"source": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(event.Source()),
		},
		"id": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(event.ID()),
		},
		"kind": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String("event"),
		},
		"type": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(event.Type()),
		},
		"time": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(event.Time().Format(time.RFC3339)),
		},
		"datacontenttype": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(event.ContentType()),
		},
		"iocevixeuser": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(event.User()),
		},
		"iocevixetransaction": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(event.Transaction()),
		},
	}
}
