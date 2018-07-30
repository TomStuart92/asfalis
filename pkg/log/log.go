package log

import (
	"errors"
	"fmt"
)

// Record forms the abstraction for a single key-value pair
type Record struct {
	Key   string
	Value string
}

var log LinkedList

func Initialize() (bool, error) {
	log = LinkedList{nil, nil, 0}
	_, err := log.append("./logfile.txt")
	if err != nil {
		return false, err
	}
	return true, nil
}

// returns the current tail
func getCurrentFile() (*filePointer, error) {
	if log.Tail == nil {
		return nil, errors.New("No Current Logfile")
	}
	return log.Tail.Data, nil
}

// Append appends a file to the current logfile
func Append(record Record) (bool, error) {
	file, err := getCurrentFile()
	if err != nil {
		return false, err
	}
	file.Mutex.Lock()
	_, err = fmt.Fprintf(file.File, "{%s: %s}", record.Key, record.Value)
	file.Mutex.Unlock()
	return true, nil
}
