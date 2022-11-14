package command

import "time"

type Command interface {
	Source() string
	ID() string
	Type() string
	Time() time.Time
	ContentType() string
	Data(interface{})
	User() string
	Transaction() string
}
