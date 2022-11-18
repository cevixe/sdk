package command

import (
	"time"

	"github.com/cevixe/sdk/common/json"
	"github.com/relvacode/iso8601"
)

type impl struct {
	CommandSource      string `json:"source"`
	CommandID          string `json:"id"`
	CommandType        string `json:"type"`
	CommandTime        string `json:"time"`
	CommandContentType string `json:"datacontenttype"`
	CommandData        string `json:"data"`
	CommandUser        string `json:"iocevixeuser"`
	CommandTransaction string `json:"iocevixetransaction"`
}

func (e *impl) Source() string {
	return e.CommandSource
}

func (c *impl) ID() string {
	return c.CommandID
}

func (c *impl) Type() string {
	return c.CommandType
}

func (c *impl) Time() time.Time {
	time, _ := iso8601.ParseString(c.CommandTime)
	return time
}

func (c *impl) ContentType() string {
	return c.CommandContentType
}

func (c *impl) Data(obj interface{}) {
	json.Unmarshal(c.CommandData, obj)
}

func (c *impl) User() string {
	return c.CommandUser
}

func (c *impl) Transaction() string {
	return c.CommandTransaction
}
