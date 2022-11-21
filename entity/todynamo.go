package entity

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

func ToDynamodb_Map(entity Entity) (map[string]types.AttributeValue, error) {
	impl := entity.(*entityImpl)
	item, err := attributevalue.MarshalMap(impl.EntityData)
	if err != nil {
		return nil, errors.Wrap(err, "cannot generate dynamo map from entity data")
	}

	item["__typename"] = &types.AttributeValueMemberS{Value: impl.EntityType}
	item["id"] = &types.AttributeValueMemberS{Value: impl.EntityID}
	item["version"] = &types.AttributeValueMemberN{Value: strconv.FormatUint(impl.EntityVersion, 10)}
	item["__status"] = &types.AttributeValueMemberS{Value: string(impl.EntityStatus)}
	item["__space"] = &types.AttributeValueMemberS{Value: fmt.Sprintf("%s#%s", impl.EntityStatus, impl.EntityType)}
	item["updatedAt"] = &types.AttributeValueMemberS{Value: impl.EntityUpdatedAt.Format(time.RFC3339)}
	item["updatedBy"] = &types.AttributeValueMemberN{Value: impl.EntityUpdatedBy}
	item["createdAt"] = &types.AttributeValueMemberS{Value: impl.EntityCreatedAt.Format(time.RFC3339)}
	item["createdBy"] = &types.AttributeValueMemberN{Value: impl.EntityCreatedBy}

	item["__transaction"] = &types.AttributeValueMemberS{Value: impl.LastTransaction}
	if impl.LastEventTrigger != "" {
		item["__eventtrigger"] = &types.AttributeValueMemberS{Value: impl.LastEventTrigger}
	} else {
		item["__eventtrigger"] = &types.AttributeValueMemberNULL{Value: true}
	}
	if impl.LastEventType != "" {
		item["__eventtype"] = &types.AttributeValueMemberS{Value: impl.LastEventType}
	} else {
		item["__eventtype"] = &types.AttributeValueMemberNULL{Value: true}
	}
	if impl.LastEventVersion > 0 {
		item["__eventversion"] = &types.AttributeValueMemberN{Value: strconv.FormatUint(impl.EntityVersion, 10)}
	} else {
		item["__eventversion"] = &types.AttributeValueMemberNULL{Value: true}
	}
	if impl.LastEventData != nil {
		eventData, err := attributevalue.MarshalMap(impl.LastEventData)
		if err != nil {
			return nil, errors.Wrap(err, "cannot generate dynamo map from last event data")
		}
		item["__eventdata"] = &types.AttributeValueMemberM{Value: eventData}
	} else {
		item["__eventdata"] = &types.AttributeValueMemberNULL{Value: true}
	}
	return item, nil
}
