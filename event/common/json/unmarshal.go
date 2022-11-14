package json

import (
	"encoding/json"
	"log"
)

func Unmarshal(value string, obj interface{}) {

	err := json.Unmarshal([]byte(value), obj)
	if err != nil {
		log.Fatalf("cannot unmarshall json to object: %v", err)
	}
}
