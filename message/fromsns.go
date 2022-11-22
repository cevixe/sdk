package message

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/pkg/errors"
	"github.com/relvacode/iso8601"
)

func FromSNS(input *events.SNSEntity) (Message, error) {
	msg := &messageImpl{}

	messageSource, err := getSNSEntityStringAttribute(input, "source")
	if err != nil {
		return nil, errors.Wrap(err, "message source not found")
	}

	messageID, err := getSNSEntityStringAttribute(input, "id")
	if err != nil {
		return nil, errors.Wrap(err, "message id not found")
	}

	messageKind, err := getSNSEntityStringAttribute(input, "kind")
	if err != nil {
		return nil, errors.Wrap(err, "message kind not found")
	}
	if MessageKind(messageKind) != MessageKind_Event &&
		MessageKind(messageKind) != MessageKind_Command {
		return nil, errors.Wrap(err, "message kind not valid")
	}

	messageType, err := getSNSEntityStringAttribute(input, "type")
	if err != nil {
		return nil, errors.Wrap(err, "message type not found")
	}

	messageTimeString, err := getSNSEntityStringAttribute(input, "time")
	if err != nil {
		return nil, errors.Wrap(err, "message time not found")
	}
	messageTime, err := iso8601.ParseString(messageTimeString)
	if err != nil {
		return nil, errors.Wrap(err, "message time has no valid format")
	}

	messageContentType, err := getSNSEntityStringAttribute(input, "contentType")
	if err != nil {
		return nil, errors.Wrap(err, "message data content type not found")
	}

	messageEncodingType, err := getSNSEntityStringAttribute(input, "encodingType")
	if err != nil {
		return nil, errors.Wrap(err, "message data encoding type not found")
	}

	messageData := make(map[string]interface{})
	if err = json.Unmarshal([]byte(input.Message), &messageData); err != nil {
		return nil, errors.Wrap(err, "cannot unmarshal sns message")
	}

	messageAuthor, err := getSNSEntityStringAttribute(input, "author")
	if err != nil {
		return nil, errors.Wrap(err, "message user not found")
	}

	messageTransaction, err := getSNSEntityStringAttribute(input, "transaction")
	if err != nil {
		return nil, errors.Wrap(err, "message transaction not found")
	}
	messageTrigger, _ := getSNSEntityStringAttribute(input, "transaction")

	msg.MessageSource = messageSource
	msg.MessageID = messageID
	msg.MessageKind = MessageKind(messageKind)
	msg.MessageType = messageType
	msg.MessageTime = messageTime
	msg.MessageContentType = messageContentType
	msg.MessageEncodingType = messageEncodingType
	msg.MessageData = messageData
	msg.MessageAuthor = messageAuthor
	msg.MessageTrigger = messageTrigger
	msg.MessageTransaction = messageTransaction

	return msg, nil
}

func getSNSEntityStringAttribute(input *events.SNSEntity, name string) (string, error) {
	attribute := input.MessageAttributes[name]
	if attribute == nil {
		return "", errors.New("sns message attribute not found")
	}
	attributeMap := attribute.(map[string]interface{})
	if attributeMap["Type"] != "String" {
		return "", errors.New("invalid sns message attribute type")
	}
	if attributeMap["Value"] == nil {
		return "", errors.New("sns message attribute not found")
	}
	value := attributeMap["Value"].(string)
	return value, nil
}
