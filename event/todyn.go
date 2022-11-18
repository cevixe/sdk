package event

import (
	"encoding/json"
	"log"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

func To_DynamodbWriteRequest(event Event) (*types.WriteRequest, error) {

	itemJsonBuffer, err := json.Marshal(event)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal event as json")
	}

	itemMap := make(Map)
	if err = json.Unmarshal(itemJsonBuffer, &itemMap); err != nil {
		return nil, errors.Wrap(err, "cannot marshal event as map")
	}

	item, err := attributevalue.MarshalMap(itemMap)
	if err != nil {
		log.Fatalf("cannot marshal event to dynamodb map: %v", err)
	}

	request := &types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: item,
		},
	}

	return request, nil
}
