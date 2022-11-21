package message

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

func ToDynamodb_Map(msg Message) (map[string]types.AttributeValue, error) {
	impl := msg.(*messageImpl)
	item, err := attributevalue.MarshalMap(impl)
	if err != nil {
		return nil, errors.Wrap(err, "cannot generate dynamo map from entity data")
	}

	return item, nil
}
