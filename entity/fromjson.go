package entity

import (
	"encoding/json"

	"github.com/pkg/errors"
)

func FromJson(input []byte) (Entity, error) {
	entity := &entityImpl{}
	err := json.Unmarshal(input, entity)
	if err != nil {
		return nil, errors.Wrap(err, "cannot unmarshal json to entity")
	}
	return entity, nil
}
