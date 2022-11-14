package event

import "time"

type Event interface {
	Source() string
	ID() string
	Type() string
	Time() time.Time
	ContentType() string
	Data(interface{})
	User() string
	Transaction() string
}
