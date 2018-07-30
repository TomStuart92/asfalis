package core

import (
	log "github.com/TomStuart92/asfalis/pkg/log"
)

// Insert appends a single key, value pair to the latest log file
func Insert(key string, value string) (log.Record, error) {
	record := log.Record{Key: key, Value: value}
	ok, err := log.Append(record)
	if !ok {
		return record, err
	}
	return record, nil
}
