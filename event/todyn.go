package event

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func To_DynamodbWriteRequest(event Event) types.WriteRequest {

	item, err := attributevalue.MarshalMap(event)
	if err != nil {
		log.Fatalf("cannot marshal event to dynamodb map: %v", err)
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: item,
		},
	}
}
