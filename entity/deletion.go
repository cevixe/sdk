package entity

import (
	"context"
	"time"

	cvxcontext "github.com/cevixe/sdk/context"
)

type Deletion interface {
	SetEvent(
		eventType string,
		eventVersion uint64,
		eventData interface{},
	) Deletion

	Execute() Entity
}

type deletionImpl struct {
	Author          string
	Trigger         string
	Transaction     string
	Target          *entityImpl
	NewEventType    string
	NewEventVersion uint64
	NewEventData    interface{}
}

func newDeletion(ctx context.Context, target *entityImpl) Deletion {
	cvx := cvxcontext.GetExecutionContenxt(ctx)
	return &deletionImpl{
		Author:      cvx.Author,
		Trigger:     cvx.Trigger,
		Transaction: cvx.Transaction,
		Target:      target,
	}
}

func (d *deletionImpl) SetEvent(
	eventType string,
	eventVersion uint64,
	eventData interface{},
) Deletion {
	d.NewEventType = eventType
	if eventVersion == 0 {
		d.NewEventVersion = 1
	} else {
		d.NewEventVersion = eventVersion
	}
	d.NewEventData = eventData
	return d
}

func (d *deletionImpl) Execute() Entity {
	return &entityImpl{
		EntityID:         d.Target.ID(),
		EntityType:       d.Target.Type(),
		EntityVersion:    d.Target.Version() + 1,
		EntityStatus:     EntityStatus_Dead,
		EntityData:       d.Target.EntityData,
		EntityUpdatedBy:  d.Author,
		EntityUpdatedAt:  time.Now(),
		EntityCreatedAt:  d.Target.CreatedAt(),
		EntityCreatedBy:  d.Target.CreatedBy(),
		LastTransaction:  d.Transaction,
		LastEventTrigger: d.Trigger,
		LastEventType:    d.NewEventType,
		LastEventVersion: d.NewEventVersion,
		LastEventData:    d.NewEventData,
	}
}
