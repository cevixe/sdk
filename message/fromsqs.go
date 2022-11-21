package message

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/pkg/errors"
)

func FromSQS(message events.SQSMessage) (Message, error) {

	record := &events.SNSEntity{}
	buffer := []byte(message.Body)
	err := json.Unmarshal(buffer, record)
	if err != nil {
		return nil, errors.Wrap(err, "cannot unmarshal sqs body")
	}

	return FromSNS(record)
}
