package json

import (
	"encoding/json"
	"log"
)

func Marshal(data interface{}) string {

	jsonString, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("cannot marshall object to json: %v", err)
	}

	return string(jsonString)
}
