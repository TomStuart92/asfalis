package visulization

import (
	"fmt"
)

func RecordEvent(id string, data interface{}) error {
	fmt.Printf("Event %s: %+v", id, data)
	return nil
}