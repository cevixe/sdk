package command

import (
	"fmt"
	"time"

	"github.com/cevixe/sdk/common/json"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/jsii-runtime-go"
)

func To_SNSPublishBatchRequestEntry(command Command) types.PublishBatchRequestEntry {

	data := make(map[string]interface{})
	command.Data(&data)
	message := json.Marshal(data)
	entryId := fmt.Sprintf("%s/%s", command.Source(), command.ID())

	return types.PublishBatchRequestEntry{
		Id:                     jsii.String(entryId),
		Subject:                jsii.String(command.Type()),
		MessageGroupId:         jsii.String(command.Source()),
		MessageDeduplicationId: jsii.String(command.ID()),
		Message:                jsii.String(message),
		MessageAttributes:      generateMessageAttributes(command),
	}
}

func To_SNSPublishInput(command Command) sns.PublishInput {

	data := make(map[string]interface{})
	command.Data(&data)
	message := json.Marshal(data)

	return sns.PublishInput{
		Subject:                jsii.String(command.Type()),
		MessageGroupId:         jsii.String(command.Source()),
		MessageDeduplicationId: jsii.String(command.ID()),
		Message:                jsii.String(message),
		MessageAttributes:      generateMessageAttributes(command),
	}
}

func generateMessageAttributes(command Command) map[string]types.MessageAttributeValue {

	return map[string]types.MessageAttributeValue{
		"source": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(command.Source()),
		},
		"id": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(command.ID()),
		},
		"kind": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String("command"),
		},
		"type": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(command.Type()),
		},
		"time": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(command.Time().Format(time.RFC3339)),
		},
		"datacontenttype": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(command.ContentType()),
		},
		"iocevixeuser": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(command.User()),
		},
		"iocevixetransaction": {
			DataType:    jsii.String("String"),
			StringValue: jsii.String(command.Transaction()),
		},
	}
}
