package command

import "github.com/aws/aws-lambda-go/events"

func From_SNS(record *events.SNSEventRecord) Command {
	command := &impl{}
	message := record.SNS

	command.CommandSource = message.MessageAttributes["source"].(string)
	command.CommandID = message.MessageAttributes["id"].(string)
	command.CommandType = message.MessageAttributes["type"].(string)
	command.CommandTime = message.MessageAttributes["time"].(string)
	command.CommandContentType = message.MessageAttributes["datacontenttype"].(string)
	command.CommandUser = message.MessageAttributes["iocevixeuser"].(string)
	command.CommandTransaction = message.MessageAttributes["iocevixetransaction"].(string)

	command.CommandData = message.Message

	return command
}
