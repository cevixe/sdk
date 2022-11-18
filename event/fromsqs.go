package event

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
)

func From_SQSMessage(message events.SQSMessage) Event {
	jsonString, _ := json.Marshal(message)
	fmt.Println(string(jsonString))
	record := &events.SNSEntity{}
	buffer := []byte(message.Body)
	_ = json.Unmarshal(buffer, record)

	return From_SNSEntity(record)
}
