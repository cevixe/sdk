package iso8601

import (
	"time"
)

func Valid(value string) bool {

	_, err := time.Parse(Layout, value)
	return err == nil
}
