package message

import (
	"encoding/json"

	"github.com/pkg/errors"
)

func FromJson(input []byte) (Message, error) {
	msg := &messageImpl{}
	err := json.Unmarshal(input, msg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot unmarshal json to message")
	}
	return msg, nil
}
