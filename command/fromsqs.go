package command

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
)

func From_SQSMessage(message events.SQSMessage) Command {
	record := &events.SNSEventRecord{}
	buffer := []byte(message.Body)
	_ = json.Unmarshal(buffer, record)
	return From_SNS(record)
}
