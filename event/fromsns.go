package event

import (
	"github.com/aws/aws-lambda-go/events"
)

func From_SNS(record *events.SNSEventRecord) Event {
	event := &impl{}
	message := record.SNS

	event.EventSource = message.MessageAttributes["source"].(string)
	event.EventID = message.MessageAttributes["id"].(string)
	event.EventType = message.MessageAttributes["type"].(string)
	event.EventTime = message.MessageAttributes["time"].(string)
	event.EventContentType = message.MessageAttributes["datacontenttype"].(string)
	event.EventUser = message.MessageAttributes["iocevixeuser"].(string)
	event.EventTransaction = message.MessageAttributes["iocevixetransaction"].(string)

	event.EventData = message.Message

	return event
}
