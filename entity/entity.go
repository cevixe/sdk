package entity

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cevixe/sdk/message"
	"github.com/pkg/errors"
	"github.com/stoewer/go-strcase"
)

type Entity interface {
	ID() string
	Type() string
	Version() uint64
	Indexes() []string
	Status() EntityStatus
	Data(interface{}) error
	UpdatedAt() time.Time
	UpdatedBy() string
	CreatedAt() time.Time
	CreatedBy() string
	LastEvent() (message.Event, error)
}

type entityImpl struct {
	EntityType       string       `json:"type"`
	EntityID         string       `json:"id"`
	EntityVersion    uint64       `json:"version"`
	EntityStatus     EntityStatus `json:"status"`
	EntityData       interface{}  `json:"data"`
	EntityUpdatedBy  string       `json:"updatedBy"`
	EntityUpdatedAt  time.Time    `json:"updatedAt"`
	EntityCreatedBy  string       `json:"createdBy"`
	EntityCreatedAt  time.Time    `json:"createdAt"`
	EntityIndexes    []string     `json:"indexes"`
	LastTransaction  string       `json:"lastTransaction"`
	LastEventTrigger string       `json:"lastEventTrigger,omitempty"`
	LastEventType    string       `json:"lastEventType,omitempty"`
	LastEventVersion uint64       `json:"lastEventVersion,omitempty"`
	LastEventData    interface{}  `json:"lastEventData,omitempty"`
}

type EntityStatus string

const (
	EntityStatus_Alive EntityStatus = "alive"
	EntityStatus_Dead  EntityStatus = "dead"
)

func (e *entityImpl) ID() string {
	return e.EntityID
}

func (e *entityImpl) Type() string {
	return e.EntityType
}

func (e *entityImpl) Version() uint64 {
	return e.EntityVersion
}

func (e *entityImpl) Status() EntityStatus {
	return e.EntityStatus
}

func (e *entityImpl) Indexes() []string {
	return e.EntityIndexes
}

func (e *entityImpl) Data(obj interface{}) error {
	buffer, err := json.Marshal(e.EntityData)
	if err != nil {
		return errors.Wrap(err, "cannot marshal entity data")
	}
	if err = json.Unmarshal(buffer, obj); err != nil {
		return errors.Wrap(err, "cannot unmarshal entity state")
	}
	return nil
}

func (e *entityImpl) UpdatedBy() string {
	return e.EntityUpdatedBy
}

func (e *entityImpl) UpdatedAt() time.Time {
	return e.EntityCreatedAt
}

func (e *entityImpl) CreatedBy() string {
	return e.EntityCreatedBy
}

func (e *entityImpl) CreatedAt() time.Time {
	return e.EntityCreatedAt
}

func (e *entityImpl) LastEvent() (message.Event, error) {

	eventMap := make(map[string]interface{})

	typename := strcase.KebabCase(e.EntityType)
	eType := e.LastEventType
	if eType == "" {
		if e.EntityVersion == 1 {
			eType = "created"
		} else if e.EntityStatus == EntityStatus_Dead {
			eType = "deleted"
		} else {
			eType = "updated"
		}
	}
	eVersion := e.LastEventVersion
	if eVersion == 0 {
		eVersion = 1
	}
	eData := e.LastEventData
	if eData == nil {
		eData = e.EntityData
	}

	eventMap["source"] = fmt.Sprintf("/%s/%s", typename, e.EntityID)
	eventMap["id"] = fmt.Sprintf("%020d", int(e.EntityVersion))
	eventMap["kind"] = "event"
	eventMap["type"] = fmt.Sprintf("%s.%s.v%d", typename, eType, eVersion)
	eventMap["time"] = e.EntityUpdatedAt
	eventMap["contentType"] = "application/json"
	eventMap["encodingType"] = "identity"
	eventMap["data"] = eData
	eventMap["author"] = e.EntityUpdatedBy
	eventMap["trigger"] = e.LastEventTrigger
	eventMap["transaction"] = e.LastTransaction

	eventJson, err := json.Marshal(eventMap)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal event map")
	}

	return message.FromJson(eventJson)
}

func (e *entityImpl) Mutate(ctx context.Context, newState interface{}) Mutation {
	if e.EntityStatus == EntityStatus_Dead {
		return nil
	}
	return newMutation(ctx, e, newState)
}

func (e *entityImpl) Delete(ctx context.Context) Deletion {
	if e.EntityStatus == EntityStatus_Dead {
		return nil
	}
	return newDeletion(ctx, e)
}

type EntityPage interface {
	Items() []Entity
	NextToken() string
}

type entityPageImpl struct {
	PageItems     []Entity
	PageNextToken string
}

func NewPage(entities []Entity, nextToken string) EntityPage {
	return &entityPageImpl{
		PageItems:     entities,
		PageNextToken: nextToken,
	}
}

func (e *entityPageImpl) Items() []Entity {
	return e.PageItems
}

func (e *entityPageImpl) NextToken() string {
	return e.PageNextToken
}
