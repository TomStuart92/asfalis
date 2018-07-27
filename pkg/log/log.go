package log

import (
	"errors"
	"fmt"
	"os"
)

// Record forms the abstraction for a single key-value pair
type Record struct {
	Key   string
	Value string
}

// Logfile holds the name of a logfile
type file struct {
	Name string
}

// Tail holds a pointer to the latest logfile.
type Tail struct {
	CurrentTail file
}

var initialFile = file{"./logFile.txt"}
var tail = Tail{initialFile}

func getCurrentFilename() (string, error) {
	if tail.CurrentTail.Name == "" {
		return "", errors.New("No Current Logfile")
	}
	return tail.CurrentTail.Name, nil
}

// Append appends a file to the current logfile
func Append(record Record) (Record, error) {
	filename, err := getCurrentFilename()
	if err != nil {
		return record, err
	}
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return record, err
	}
	_, err = fmt.Fprintf(f, "{%s: %s}", record.Key, record.Value)
	f.Close()
	return record, nil
}
