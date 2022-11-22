package message

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

func ToDynamodb_Map(msg Message) (map[string]types.AttributeValue, error) {
	impl := msg.(*messageImpl)

	buffer, err := json.Marshal(impl)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal message object to json")
	}

	msgMap := make(map[string]interface{})
	if err = json.Unmarshal(buffer, &msgMap); err != nil {
		return nil, errors.Wrap(err, "cannot unmarshal json message to map")
	}

	item, err := attributevalue.MarshalMap(msgMap)
	if err != nil {
		return nil, errors.Wrap(err, "cannot generate dynamo map from entity data")
	}

	return item, nil
}
