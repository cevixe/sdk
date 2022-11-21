package result

import (
	"github.com/cevixe/sdk/entity"
	"github.com/cevixe/sdk/message"
)

type Result interface {
	GetEntities() []entity.Entity
	AddEntities(entities ...entity.Entity) Result

	GetCommands() []message.Command
	AddCommands(commands ...message.Command) Result
}

func NewResult() Result {
	return &resultImpl{
		entities: make([]entity.Entity, 0),
		commands: make([]message.Command, 0),
	}
}

type resultImpl struct {
	entities []entity.Entity
	commands []message.Command
}

func (r *resultImpl) GetEntities() []entity.Entity {
	return r.entities
}

func (r *resultImpl) AddEntities(entities ...entity.Entity) Result {
	r.entities = append(r.entities, entities...)
	return r
}

func (r *resultImpl) GetCommands() []message.Command {
	return r.commands
}

func (r *resultImpl) AddCommands(commands ...message.Command) Result {
	r.commands = append(r.commands, commands...)
	return r
}
