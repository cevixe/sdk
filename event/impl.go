package event

import (
	"time"

	"github.com/cevixe/sdk/common/iso8601"
	"github.com/cevixe/sdk/common/json"
)

type impl struct {
	EventSource      string `json:"source"`
	EventID          string `json:"id"`
	EventType        string `json:"type"`
	EventTime        string `json:"time"`
	EventContentType string `json:"datacontenttype"`
	EventData        string `json:"data"`
	EventUser        string `json:"iocevixeuser"`
	EventTransaction string `json:"iocevixetransaction"`
}

func (e *impl) Source() string {
	return e.EventSource
}

func (e *impl) ID() string {
	return e.EventID
}

func (e *impl) Type() string {
	return e.EventType
}

func (e *impl) Time() time.Time {
	return iso8601.ToTime(e.EventTime)
}

func (e *impl) ContentType() string {
	return e.EventContentType
}

func (e *impl) Data(obj interface{}) {
	json.Unmarshal(e.EventData, obj)
}

func (e *impl) User() string {
	return e.EventUser
}

func (e *impl) Transaction() string {
	return e.EventTransaction
}