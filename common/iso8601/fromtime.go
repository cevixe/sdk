package iso8601

import "time"

func FromTime(value time.Time) string {
	return value.Format(Layout)
}
