package entity

import (
	"context"
	"time"

	"github.com/cevixe/sdk/common/reflect"
	cvxcontext "github.com/cevixe/sdk/context"
	"github.com/oklog/ulid/v2"
)

type Creation interface {
	SetEvent(
		eventType string,
		eventVersion uint64,
		eventData interface{},
	) Creation

	Execute() Entity
}

func Create(ctx context.Context, state interface{}) Creation {
	cvx := cvxcontext.GetExecutionContenxt(ctx)
	return &creationImpl{
		Author:      cvx.Author,
		Trigger:     cvx.Trigger,
		Transaction: cvx.Transaction,
		State:       state,
	}
}

type creationImpl struct {
	Author          string
	Trigger         string
	Transaction     string
	State           interface{}
	NewEventType    string
	NewEventVersion uint64
	NewEventData    interface{}
}

func (c *creationImpl) SetEvent(
	eventType string,
	eventVersion uint64,
	eventData interface{},
) Creation {
	c.NewEventType = eventType
	if eventVersion == 0 {
		c.NewEventVersion = 1
	} else {
		c.NewEventVersion = eventVersion
	}
	c.NewEventData = eventData
	return c
}

func (c *creationImpl) Execute() Entity {
	now := time.Now()
	return &entityImpl{
		EntityID:         ulid.Make().String(),
		EntityType:       reflect.GetTypeName(c.State),
		EntityVersion:    1,
		EntityStatus:     EntityStatus_Alive,
		EntityData:       c.State,
		EntityUpdatedBy:  c.Author,
		EntityUpdatedAt:  now,
		EntityCreatedBy:  c.Author,
		EntityCreatedAt:  now,
		LastTransaction:  c.Transaction,
		LastEventTrigger: c.Trigger,
		LastEventType:    c.NewEventType,
		LastEventVersion: c.NewEventVersion,
		LastEventData:    c.NewEventData,
	}
}
