package entity

import (
	"context"
	"time"

	cvxcontext "github.com/cevixe/sdk/context"
)

type Mutation interface {
	SetEvent(
		eventType string,
		eventVersion uint64,
		eventData interface{},
	) Mutation

	Execute() Entity
}

type mutationImpl struct {
	Author          string
	Target          Entity
	Trigger         string
	Transaction     string
	NewEventType    string
	NewEventVersion uint64
	NewEventData    interface{}
	NewEntityData   interface{}
}

func newMutation(
	ctx context.Context,
	target *entityImpl,
	newState interface{},
) Mutation {

	cvx := cvxcontext.GetExecutionContenxt(ctx)
	return &mutationImpl{
		Author:        cvx.Author,
		Trigger:       cvx.Trigger,
		Transaction:   cvx.Transaction,
		Target:        target,
		NewEntityData: newState,
	}
}

func (m *mutationImpl) SetEvent(
	eventType string,
	eventVersion uint64,
	eventData interface{},
) Mutation {
	m.NewEventType = eventType
	if eventVersion == 0 {
		m.NewEventVersion = 1
	} else {
		m.NewEventVersion = eventVersion
	}
	m.NewEventData = eventData
	return m
}

func (m *mutationImpl) Execute() Entity {
	return &entityImpl{
		EntityID:         m.Target.ID(),
		EntityType:       m.Target.Type(),
		EntityVersion:    m.Target.Version() + 1,
		EntityStatus:     EntityStatus_Alive,
		EntityData:       m.NewEntityData,
		EntityUpdatedBy:  m.Author,
		EntityUpdatedAt:  time.Now(),
		EntityCreatedAt:  m.Target.CreatedAt(),
		EntityCreatedBy:  m.Target.CreatedBy(),
		LastTransaction:  m.Transaction,
		LastEventTrigger: m.Trigger,
		LastEventType:    m.NewEventType,
		LastEventVersion: m.NewEventVersion,
		LastEventData:    m.NewEventData,
	}
}
