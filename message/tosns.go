package message

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/jsii-runtime-go"
	"github.com/pkg/errors"
)

func ToSNS_Entry(msg Message) (*types.PublishBatchRequestEntry, error) {

	data := make(map[string]interface{})
	msg.Data(&data)
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal message data")
	}
	entryId := fmt.Sprintf("%s/%s", msg.Source(), msg.ID())

	return &types.PublishBatchRequestEntry{
		Id:                jsii.String(entryId),
		Subject:           jsii.String(msg.Type()),
		MessageGroupId:    jsii.String(msg.Source()),
		Message:           jsii.String(string(payload)),
		MessageAttributes: generateMessageAttributes(msg),
	}, nil
}

func ToSNS_Input(msg Message) (*sns.PublishInput, error) {

	data := make(map[string]interface{})
	msg.Data(&data)
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal message data")
	}

	return &sns.PublishInput{
		Subject:           jsii.String(msg.Type()),
		MessageGroupId:    jsii.String(msg.Source()),
		Message:           jsii.String(string(payload)),
		MessageAttributes: generateMessageAttributes(msg),
	}, nil
}

func newStringMessageAttribute(value string) types.MessageAttributeValue {
	return types.MessageAttributeValue{
		DataType:    jsii.String("String"),
		StringValue: jsii.String(value),
	}
}

func generateMessageAttributes(msg Message) map[string]types.MessageAttributeValue {

	return map[string]types.MessageAttributeValue{
		"source":       newStringMessageAttribute(msg.Source()),
		"id":           newStringMessageAttribute(msg.ID()),
		"kind":         newStringMessageAttribute(string(msg.Kind())),
		"type":         newStringMessageAttribute(msg.Type()),
		"time":         newStringMessageAttribute(msg.Time().Format(time.RFC3339)),
		"contentType":  newStringMessageAttribute(msg.ContentType()),
		"encodingType": newStringMessageAttribute(msg.EncodingType()),
		"author":       newStringMessageAttribute(msg.Author()),
		"trigger":      newStringMessageAttribute(msg.Trigger()),
		"transaction":  newStringMessageAttribute(msg.Transaction()),
	}
}
