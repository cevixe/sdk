package event

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
)

func From_SNSEntity(message *events.SNSEntity) Event {
	event := &impl{}
	jsonString, _ := json.Marshal(message)
	fmt.Println(string(jsonString))

	event.EventSource = getSNSMessageAttributeString(message, "source")
	event.EventID = getSNSMessageAttributeString(message, "id")
	event.EventType = getSNSMessageAttributeString(message, "type")
	event.EventTime = getSNSMessageAttributeString(message, "time")
	event.EventContentType = getSNSMessageAttributeString(message, "datacontenttype")
	event.EventUser = getSNSMessageAttributeString(message, "iocevixeuser")
	event.EventTransaction = getSNSMessageAttributeString(message, "iocevixetransaction")

	event.EventData = message.Message

	return event
}

func getSNSMessageAttributeString(message *events.SNSEntity, attribute string) string {
	messageAttribute := message.MessageAttributes[attribute]
	if messageAttribute == nil {
		return ""
	}
	if messageAttribute.(Map)["Type"] != "String" {
		return ""
	}
	return messageAttribute.(Map)["Value"].(string)
}
