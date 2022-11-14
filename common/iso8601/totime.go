package iso8601

import (
	"log"
	"time"
)

func ToTime(value string) time.Time {

	time, err := time.Parse(Layout, value)
	if err != nil {
		log.Fatalf("cannot parse time string: %v", err)
	}

	return time
}
