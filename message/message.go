package message

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

type MessageKind string

const (
	MessageKind_Event   MessageKind = "event"
	MessageKind_Command MessageKind = "command"
)

type Message interface {
	Source() string
	ID() string
	Kind() MessageKind
	Type() string
	Time() time.Time
	ContentType() string
	EncodingType() string
	Data(interface{}) error
	Author() string
	Trigger() string
	Transaction() string
}

type Event = Message
type Command = Message

type messageImpl struct {
	Message
	MessageSource       string      `json:"source"`
	MessageID           string      `json:"id"`
	MessageKind         MessageKind `json:"kind"`
	MessageType         string      `json:"type"`
	MessageTime         time.Time   `json:"time"`
	MessageContentType  string      `json:"contentType"`
	MessageEncodingType string      `json:"encodingType"`
	MessageData         interface{} `json:"data"`
	MessageAuthor       string      `json:"author"`
	MessageTrigger      string      `json:"trigger"`
	MessageTransaction  string      `json:"transaction"`
}

func (c *messageImpl) Source() string {
	return c.MessageSource
}

func (c *messageImpl) ID() string {
	return c.MessageID
}

func (c *messageImpl) Type() string {
	return c.MessageType
}

func (c *messageImpl) Kind() MessageKind {
	return c.MessageKind
}

func (c *messageImpl) Time() time.Time {
	return c.MessageTime
}

func (c *messageImpl) ContentType() string {
	return c.MessageContentType
}

func (c *messageImpl) EncodingType() string {
	return c.MessageEncodingType
}

func (c *messageImpl) Data(obj interface{}) error {
	buffer, err := json.Marshal(c.MessageData)
	if err != nil {
		return errors.Wrap(err, "cannot marshal command data")
	}
	if err = json.Unmarshal(buffer, obj); err != nil {
		return errors.Wrap(err, "cannot unmarshal command state")
	}
	return nil
}

func (c *messageImpl) Author() string {
	return c.MessageAuthor
}

func (c *messageImpl) Trigger() string {
	return c.MessageTrigger
}

func (c *messageImpl) Transaction() string {
	return c.MessageTransaction
}
